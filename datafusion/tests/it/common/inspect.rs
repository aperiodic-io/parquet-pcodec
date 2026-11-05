//! What a file holds per column: codecs, pages and PCODEC payload modes. A port of
//! `rust/tparquet/src/inspect.rs` (built on the 60.1.0 fork) to the 59.3.2 fork, for the
//! tests: it reads the footer with the fork's metadata API and walks the page headers itself,
//! so that it sees the payloads (the page reader only hands out decompressed pages).

use parquet::basic::Compression;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaDataReader};
use parquet::file::reader::ChunkReader;
use std::collections::BTreeMap;
use tparquet_core::{Mode, mode_of};

type Result<T> = std::result::Result<T, String>;

/// The report of [`inspect`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReport {
    pub rows: i64,
    pub row_groups: usize,
    /// Value of the `parquet.pcodec.version` footer key, if present.
    pub pcodec_version: Option<String>,
    pub columns: Vec<ColumnReport>,
}

/// One leaf column, over all row groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReport {
    pub path: String,
    pub physical_type: String,
    /// Codecs of its column chunks, each once, in the order met.
    pub codecs: Vec<String>,
    /// Pages per page type.
    pub pages: BTreeMap<String, u64>,
    /// PCODEC pages per payload mode (`RAW`, `PCO`, `DECIMAL`, `ZSTD`), plus `UNCOMPRESSED`
    /// for V2 pages stored without compression and `EMPTY` for pages with nothing to decode.
    pub modes: BTreeMap<String, u64>,
    /// Encodings of its column chunks, each once.
    pub encodings: Vec<String>,
    /// Compressed bytes (pages with their headers), as the metadata says.
    pub compressed_bytes: i64,
}

impl FileReport {
    /// The column at `path`.
    pub fn column(&self, path: &str) -> &ColumnReport {
        self.columns.iter().find(|c| c.path == path).unwrap_or_else(|| panic!("no column {path}"))
    }

    /// `(path, codecs)` of every column.
    pub fn codecs(&self) -> Vec<(String, String)> {
        self.columns.iter().map(|c| (c.path.clone(), c.codecs.join(","))).collect()
    }
}

/// Name of a codec without its level.
fn codec_name(c: Compression) -> String {
    let s = c.to_string();
    match s.split_once('(') {
        Some((name, _)) => name.to_string(),
        None => s,
    }
}

fn mode_name(m: Mode) -> &'static str {
    match m {
        Mode::Raw => "RAW",
        Mode::Pco => "PCO",
        Mode::Decimal => "DECIMAL",
        Mode::Zstd => "ZSTD",
        _ => "UNKNOWN",
    }
}

fn page_type_name(t: i32) -> String {
    match t {
        0 => "DATA_PAGE".into(),
        1 => "INDEX_PAGE".into(),
        2 => "DICTIONARY_PAGE".into(),
        3 => "DATA_PAGE_V2".into(),
        t => format!("PAGE_TYPE_{t}"),
    }
}

/// Reads the footer and every page header of the file at `path`.
pub fn inspect_file(path: impl AsRef<std::path::Path>) -> FileReport {
    let bytes = bytes::Bytes::from(std::fs::read(path.as_ref()).unwrap());
    inspect(&bytes).unwrap()
}

/// Reads the footer and every page header of `reader`.
pub fn inspect<R: ChunkReader>(reader: &R) -> Result<FileReport> {
    let metadata = ParquetMetaDataReader::new().parse_and_finish(reader).map_err(|e| e.to_string())?;
    let file = metadata.file_metadata();
    let schema = file.schema_descr();
    let mut columns: Vec<ColumnReport> = schema
        .columns()
        .iter()
        .map(|c| ColumnReport {
            path: c.path().string(),
            physical_type: c.physical_type().to_string(),
            codecs: vec![],
            pages: BTreeMap::new(),
            modes: BTreeMap::new(),
            encodings: vec![],
            compressed_bytes: 0,
        })
        .collect();
    for rg in metadata.row_groups() {
        for (report, chunk) in columns.iter_mut().zip(rg.columns()) {
            let codec = codec_name(chunk.compression());
            if !report.codecs.contains(&codec) {
                report.codecs.push(codec);
            }
            for e in chunk.encodings() {
                let e = e.to_string();
                if !report.encodings.contains(&e) {
                    report.encodings.push(e);
                }
            }
            report.compressed_bytes += chunk.compressed_size();
            scan_pages(reader, chunk, report)?;
        }
    }
    let pcodec_version = file
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|kv| kv.key == tparquet_core::KV_KEY))
        .map(|kv| kv.value.clone().unwrap_or_default());
    Ok(FileReport { rows: file.num_rows(), row_groups: metadata.num_row_groups(), pcodec_version, columns })
}

fn scan_pages<R: ChunkReader>(reader: &R, chunk: &ColumnChunkMetaData, report: &mut ColumnReport) -> Result<()> {
    let start = chunk.dictionary_page_offset().unwrap_or(chunk.data_page_offset());
    let start = u64::try_from(start).map_err(|e| e.to_string())?;
    let len = usize::try_from(chunk.compressed_size()).map_err(|e| e.to_string())?;
    let bytes = reader.get_bytes(start, len).map_err(|e| e.to_string())?;
    let pcodec = matches!(chunk.compression(), Compression::PCODEC(_));
    let mut pos = 0;
    while pos < bytes.len() {
        let mut p = Compact::new(&bytes[pos..]);
        let h = p.page_header()?;
        let body_start = pos + p.consumed();
        let body_end = body_start + usize::try_from(h.compressed_size).map_err(|e| e.to_string())?;
        if body_end > bytes.len() {
            return Err(format!("column {}: page runs past the column chunk", report.path));
        }
        *report.pages.entry(page_type_name(h.page_type)).or_default() += 1;
        if pcodec {
            let mode = page_mode(&h, &bytes[body_start..body_end]);
            *report.modes.entry(mode.to_string()).or_default() += 1;
        }
        pos = body_end;
    }
    Ok(())
}

/// The payload mode of one PCODEC page.
fn page_mode(h: &PageHeader, body: &[u8]) -> &'static str {
    let (levels, compressed) = h.v2.unwrap_or((0, true));
    if !compressed {
        return "UNCOMPRESSED";
    }
    if i64::from(h.uncompressed_size) <= i64::from(levels) {
        return "EMPTY";
    }
    let payload = usize::try_from(levels).ok().and_then(|l| body.get(l..)).unwrap_or_default();
    mode_of(payload).map_or("INVALID", mode_name)
}

/// The fields of a Thrift `PageHeader` this module needs.
#[derive(Debug, PartialEq, Eq)]
pub struct PageHeader {
    pub page_type: i32,
    pub uncompressed_size: i32,
    pub compressed_size: i32,
    /// V2 pages: bytes of levels before the values, and `is_compressed`.
    pub v2: Option<(i32, bool)>,
}

/// The page headers of the column chunk `chunk`: `(offset of the payload in the file, header)`.
pub fn page_headers(file: &[u8], chunk: &ColumnChunkMetaData) -> Vec<(usize, PageHeader)> {
    let start = chunk.dictionary_page_offset().unwrap_or(chunk.data_page_offset()) as usize;
    let end = start + chunk.compressed_size() as usize;
    let mut pos = start;
    let mut out = vec![];
    while pos < end {
        let mut p = Compact::new(&file[pos..end]);
        let h = p.page_header().unwrap();
        let body = pos + p.consumed();
        pos = body + h.compressed_size as usize;
        out.push((body, h));
    }
    out
}

/// A reader of the Thrift compact protocol, just enough for page headers.
struct Compact<'a> {
    buf: &'a [u8],
    len: usize,
}

type Parse<T> = std::result::Result<T, String>;

impl<'a> Compact<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, len: buf.len() }
    }

    fn consumed(&self) -> usize {
        self.len - self.buf.len()
    }

    fn byte(&mut self) -> Parse<u8> {
        let (&b, rest) = self.buf.split_first().ok_or("truncated")?;
        self.buf = rest;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Parse<()> {
        self.buf = self.buf.get(n..).ok_or("truncated")?;
        Ok(())
    }

    fn varint(&mut self) -> Parse<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err("varint too long".into())
    }

    fn zigzag(&mut self) -> Parse<i64> {
        let v = self.varint()?;
        Ok((v >> 1) as i64 ^ -((v & 1) as i64))
    }

    fn i32(&mut self) -> Parse<i32> {
        i32::try_from(self.zigzag()?).map_err(|_| "i32 out of range".into())
    }

    fn read_struct(&mut self, mut field: impl FnMut(&mut Self, i16, u8) -> Parse<bool>) -> Parse<()> {
        let mut last: i16 = 0;
        loop {
            let b = self.byte()?;
            if b == 0 {
                break;
            }
            let t = b & 0x0f;
            let delta = i16::from(b >> 4);
            let id = if delta == 0 { i16::try_from(self.zigzag()?).map_err(|_| "field id")? } else { last + delta };
            last = id;
            if !field(self, id, t)? {
                self.skip(t)?;
            }
        }
        Ok(())
    }

    fn skip(&mut self, t: u8) -> Parse<()> {
        match t {
            1 | 2 => Ok(()),
            3 => self.take(1),
            4..=6 => self.varint().map(drop),
            7 => self.take(8),
            8 => {
                let n = usize::try_from(self.varint()?).map_err(|_| "length out of range")?;
                self.take(n)
            }
            9 | 10 => {
                let h = self.byte()?;
                let n = match h >> 4 {
                    15 => self.varint()?,
                    n => u64::from(n),
                };
                for _ in 0..n {
                    self.element(h & 0x0f)?;
                }
                Ok(())
            }
            11 => {
                let n = self.varint()?;
                if n == 0 {
                    return Ok(());
                }
                let kv = self.byte()?;
                for _ in 0..n {
                    self.element(kv >> 4)?;
                    self.element(kv & 0x0f)?;
                }
                Ok(())
            }
            12 => self.read_struct(|_, _, _| Ok(false)),
            t => Err(format!("unknown field type {t}")),
        }
    }

    fn element(&mut self, t: u8) -> Parse<()> {
        match t {
            1 | 2 => self.take(1),
            t => self.skip(t),
        }
    }

    fn page_header(&mut self) -> Parse<PageHeader> {
        let mut h = PageHeader { page_type: -1, uncompressed_size: -1, compressed_size: -1, v2: None };
        self.read_struct(|p, id, t| match (id, t) {
            (1, 5) => p.i32().map(|v| h.page_type = v).map(|()| true),
            (2, 5) => p.i32().map(|v| h.uncompressed_size = v).map(|()| true),
            (3, 5) => p.i32().map(|v| h.compressed_size = v).map(|()| true),
            (8, 12) => {
                let (mut def, mut rep, mut compressed) = (0, 0, true);
                p.read_struct(|p, id, t| match (id, t) {
                    (5, 5) => p.i32().map(|v| def = v).map(|()| true),
                    (6, 5) => p.i32().map(|v| rep = v).map(|()| true),
                    (7, 1 | 2) => {
                        compressed = t == 1;
                        Ok(true)
                    }
                    _ => Ok(false),
                })?;
                h.v2 = Some((def + rep, compressed));
                Ok(true)
            }
            _ => Ok(false),
        })?;
        if h.page_type < 0 || h.uncompressed_size < 0 || h.compressed_size < 0 {
            return Err("missing or negative required field".into());
        }
        Ok(h)
    }
}
