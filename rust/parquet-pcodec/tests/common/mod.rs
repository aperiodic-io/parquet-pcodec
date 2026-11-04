//! Helpers shared by the integration tests.
#![allow(dead_code)]

use arrow_array::RecordBatch;
use bytes::Bytes;
use parquet_pcodec::inspect::{FileReport, inspect};
use parquet_pcodec::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_pcodec::{ColumnCodec, Writer, WriterOptions, canonical};

/// Writes `batches` with `options`; returns the file and the codecs the writer chose.
pub fn write(batches: &[RecordBatch], options: WriterOptions) -> (Bytes, Option<Vec<ColumnCodec>>) {
    let mut out = Vec::new();
    let mut w = Writer::try_new(&mut out, batches[0].schema(), options).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    let codecs = w.codecs();
    w.close().unwrap();
    (Bytes::from(out), codecs)
}

/// The canonical JSON lines of a file.
pub fn lines(file: &Bytes) -> Vec<String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap().build().unwrap();
    reader.flat_map(|b| canonical::batch_lines(&b.unwrap()).unwrap()).collect()
}

/// The canonical JSON lines of batches.
pub fn batch_lines(batches: &[RecordBatch]) -> Vec<String> {
    batches.iter().flat_map(|b| canonical::batch_lines(b).unwrap()).collect()
}

pub fn report(file: &Bytes) -> FileReport {
    inspect(file).unwrap()
}

/// `(path, codecs joined)` per leaf column.
pub fn codecs(file: &Bytes) -> Vec<(String, String)> {
    report(file).columns.into_iter().map(|c| (c.path, c.codecs.join(","))).collect()
}

/// A deterministic pseudo-random generator (splitmix64).
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}
