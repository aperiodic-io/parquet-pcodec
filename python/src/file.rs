//! Reading: an open Parquet file (PCODEC or not), its metadata, and streams of its rows.

use crate::errors::{detached, from_arrow, from_parquet, from_pcodec, invalid, os_error};
use crate::ffi::{ArrowSchema, RecordBatchStream};
use crate::options::codec_name;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use bytes::Bytes;
use parquet_pcodec::parquet::arrow::ProjectionMask;
use parquet_pcodec::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet_pcodec::parquet::errors::Result as ParquetResult;
use parquet_pcodec::parquet::file::reader::{ChunkReader, Length};
use parquet_pcodec::parquet::file::statistics::Statistics;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyDict;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

/// Where the file's bytes are.
#[derive(Clone)]
enum Source {
    /// Opened again for every read, so that readers in other threads do not share a cursor.
    Path(PathBuf),
    Bytes(Bytes),
}

/// One open handle on a [`Source`].
pub enum Reader {
    File(std::fs::File),
    Bytes(Bytes),
}

impl Length for Reader {
    fn len(&self) -> u64 {
        match self {
            Self::File(f) => Length::len(f),
            Self::Bytes(b) => Length::len(b),
        }
    }
}

impl ChunkReader for Reader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Ok(match self {
            Self::File(f) => Box::new(f.get_read(start)?),
            Self::Bytes(b) => Box::new(b.get_read(start)?),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        match self {
            Self::File(f) => f.get_bytes(start, length),
            Self::Bytes(b) => b.get_bytes(start, length),
        }
    }
}

impl Source {
    fn open(&self) -> PyResult<Reader> {
        Ok(match self {
            Self::Path(p) => Reader::File(std::fs::File::open(p).map_err(|e| os_error(&e, p))?),
            Self::Bytes(b) => Reader::Bytes(b.clone()),
        })
    }
}

/// A Parquet file whose footer has been read.
#[pyclass(frozen, module = "parquet_pcodec._lib")]
pub struct File {
    source: Source,
    metadata: ArrowReaderMetadata,
}

impl File {
    fn load(py: Python<'_>, source: Source) -> PyResult<Self> {
        detached(py, || {
            let reader = source.open()?;
            let metadata = ArrowReaderMetadata::load(&reader, ArrowReaderOptions::new()).map_err(from_parquet)?;
            Ok(Self { source, metadata })
        })
    }

    pub fn reader(&self) -> PyResult<Reader> {
        self.source.open()
    }
}

/// Opens the Parquet file at `path`.
#[pyfunction]
pub fn open_path(py: Python<'_>, path: PathBuf) -> PyResult<File> {
    File::load(py, Source::Path(path))
}

/// Opens a Parquet file held in `data`.
#[pyfunction]
pub fn open_bytes(py: Python<'_>, data: PyBackedBytes) -> PyResult<File> {
    File::load(py, Source::Bytes(Bytes::from_owner(data)))
}

/// Batches of a projected reader with the columns in the requested order.
struct Reordered {
    inner: ParquetRecordBatchReader,
    order: Vec<usize>,
    schema: SchemaRef,
}

impl Iterator for Reordered {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|b| b.and_then(|b| b.project(&self.order)))
    }
}

impl RecordBatchReader for Reordered {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// The root indices of `columns` in the file, in request order.
fn roots(schema: &arrow_schema::Schema, columns: &[String]) -> PyResult<Vec<usize>> {
    let mut indices = Vec::with_capacity(columns.len());
    for name in columns {
        let i = schema.index_of(name).map_err(|_| invalid(format!("no column named '{name}'")))?;
        if indices.contains(&i) {
            return Err(invalid(format!("column '{name}' is requested more than once")));
        }
        indices.push(i);
    }
    Ok(indices)
}

#[pymethods]
impl File {
    /// The Arrow schema of the file.
    fn schema(&self) -> ArrowSchema {
        ArrowSchema(self.metadata.schema().clone())
    }

    /// A stream of the file's rows: the given top-level `columns` in that order, the given
    /// `row_groups` in that order, at most `limit` rows.
    #[pyo3(signature = (columns=None, row_groups=None, batch_size=65536, limit=None))]
    fn stream(
        &self,
        columns: Option<Vec<String>>,
        row_groups: Option<Vec<i64>>,
        batch_size: i64,
        limit: Option<i64>,
    ) -> PyResult<RecordBatchStream> {
        let batch_size = usize::try_from(batch_size)
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| invalid(format!("batch_size must be positive, got {batch_size}")))?;
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(self.reader()?, self.metadata.clone())
            .with_batch_size(batch_size);
        if let Some(rgs) = row_groups {
            let n = self.metadata.metadata().num_row_groups();
            let rgs = rgs
                .iter()
                .map(|&rg| {
                    usize::try_from(rg)
                        .ok()
                        .filter(|i| *i < n)
                        .ok_or_else(|| invalid(format!("row group {rg} out of range (the file has {n})")))
                })
                .collect::<PyResult<Vec<_>>>()?;
            builder = builder.with_row_groups(rgs);
        }
        if let Some(limit) = limit {
            let limit =
                usize::try_from(limit).map_err(|_| invalid(format!("limit must not be negative, got {limit}")))?;
            builder = builder.with_limit(limit);
        }
        // the reader's own schema lacks the file's schema metadata: use the file's, projected
        let file_schema = self.metadata.schema();
        let wanted = match columns {
            Some(columns) => roots(file_schema, &columns)?,
            None => (0..file_schema.fields().len()).collect(),
        };
        let mut sorted = wanted.clone();
        sorted.sort_unstable();
        let order: Vec<usize> = wanted.iter().map(|i| sorted.binary_search(i).unwrap_or_default()).collect();
        let schema = Arc::new(file_schema.project(&sorted).and_then(|s| s.project(&order)).map_err(from_arrow)?);
        let mask = ProjectionMask::roots(self.metadata.parquet_schema(), sorted);
        let inner = builder.with_projection(mask).build().map_err(from_parquet)?;
        Ok(RecordBatchStream::new(Box::new(Reordered { inner, order, schema })))
    }

    /// The footer as a dict: rows, row groups, per leaf column its physical type and codecs.
    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let md = self.metadata.metadata();
        let file = md.file_metadata();
        let d = PyDict::new(py);
        d.set_item("num_rows", file.num_rows())?;
        d.set_item("row_group_rows", md.row_groups().iter().map(|rg| rg.num_rows()).collect::<Vec<_>>())?;
        d.set_item("created_by", file.created_by())?;
        let kv: Vec<(String, Option<String>)> = file
            .key_value_metadata()
            .map(|kv| kv.iter().map(|kv| (kv.key.clone(), kv.value.clone())).collect())
            .unwrap_or_default();
        d.set_item("key_value_metadata", kv)?;
        let columns = file
            .schema_descr()
            .columns()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut codecs: Vec<String> = vec![];
                for rg in md.row_groups() {
                    let name = codec_name(rg.column(i).compression());
                    if !codecs.contains(&name) {
                        codecs.push(name);
                    }
                }
                (c.path().string(), c.physical_type().to_string(), codecs)
            })
            .collect::<Vec<_>>();
        d.set_item("columns", columns)?;
        Ok(d)
    }

    /// Per row group, the bounds its statistics give for each `INT32` and `INT64` leaf:
    /// `{path: (min, max)}`, the values as stored (signed). Leaves without both bounds are left
    /// out.
    fn integer_bounds(&self) -> Vec<BTreeMap<String, (i64, i64)>> {
        let md = self.metadata.metadata();
        md.row_groups()
            .iter()
            .map(|rg| {
                rg.columns()
                    .iter()
                    .filter_map(|c| {
                        let bounds = match c.statistics()? {
                            Statistics::Int32(s) => (i64::from(*s.min_opt()?), i64::from(*s.max_opt()?)),
                            Statistics::Int64(s) => (*s.min_opt()?, *s.max_opt()?),
                            _ => return None,
                        };
                        Some((c.column_path().string(), bounds))
                    })
                    .collect()
            })
            .collect()
    }

    /// The `parquet-pcodec inspect --json` report.
    fn inspect(&self, py: Python<'_>) -> PyResult<String> {
        detached(py, || {
            let report = parquet_pcodec::inspect::inspect(&self.reader()?).map_err(from_pcodec)?;
            Ok(report.to_json().to_string())
        })
    }
}

/// Where `a` and `b` first differ (floats compared bit for bit), or `None`.
#[pyfunction]
pub fn verify(py: Python<'_>, a: &File, b: &File) -> PyResult<Option<String>> {
    detached(py, || {
        let d = parquet_pcodec::verify::verify(a.reader()?, b.reader()?).map_err(from_pcodec)?;
        Ok(d.map(|d| d.to_string()))
    })
}
