//! Writing: the streaming [`Writer`] and [`transcode`].

use crate::errors::{Pending, detached, from_pcodec, invalid, locked};
use crate::ffi::{import_batch, import_schema, import_stream};
use crate::file::File;
use crate::options::{Options, base_codec, codec_map, codec_name};
use crate::sink::Sink;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet_pcodec::Target;
use parquet_pcodec::parquet::file::metadata::ParquetMetaData;
use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

type Codecs = BTreeMap<String, String>;

enum State {
    Open(Box<parquet_pcodec::Writer<Sink>>),
    /// Closed (with the codecs, if they were chosen) or aborted.
    Closed(Option<Codecs>),
}

/// Writes record batches to one Parquet file.
#[pyclass(frozen, module = "parquet_pcodec._lib")]
pub struct Writer {
    schema: SchemaRef,
    state: Mutex<State>,
    pending: Pending,
}

impl Writer {
    fn create(py: Python<'_>, sink: &Bound<'_, PyAny>, schema: SchemaRef, options: &Options) -> PyResult<Self> {
        let options = options.writer_options()?;
        let pending = Pending::default();
        let sink = Sink::new(sink, &pending)?;
        let s = schema.clone();
        let w = detached(py, move || parquet_pcodec::Writer::try_new(sink, s, options).map_err(from_pcodec))
            .map_err(|e| pending.or(e))?;
        Ok(Self { schema, state: Mutex::new(State::Open(Box::new(w))), pending })
    }

    /// Runs `f` on the open writer with the GIL released; an exception raised by a Python
    /// sink is re-raised as it was.
    fn with_open<T: Send>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&mut parquet_pcodec::Writer<Sink>) -> PyResult<T> + Send,
    ) -> PyResult<T> {
        locked(py, &self.state, |state| match state {
            State::Open(w) => f(w),
            State::Closed(_) => Err(closed()),
        })
        .map_err(|e| self.pending.or(e))
    }

    fn write_batches(
        &self,
        py: Python<'_>,
        batches: impl Iterator<Item = PyResult<RecordBatch>> + Send,
    ) -> PyResult<()> {
        let schema = self.schema.clone();
        self.with_open(py, move |w| {
            for batch in batches {
                let batch = batch?;
                let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
                    .map_err(|e| invalid(format!("the data does not match the writer's schema ({schema}): {e}")))?;
                w.write(&batch).map_err(from_pcodec)?;
            }
            Ok(())
        })
    }
}

#[pymethods]
impl Writer {
    /// A writer of `schema` (an `arrow_schema` capsule) to `sink` (a `str` path or a binary
    /// file-like object).
    #[new]
    fn new(py: Python<'_>, sink: &Bound<'_, PyAny>, schema: &Bound<'_, PyAny>, options: Options) -> PyResult<Self> {
        Self::create(py, sink, Arc::new(import_schema(schema)?), &options)
    }

    /// Writes every batch of an `arrow_array_stream` capsule.
    fn write_stream(&self, py: Python<'_>, stream: &Bound<'_, PyAny>) -> PyResult<()> {
        let reader = import_stream(stream)?;
        self.write_batches(py, reader.map(|b| b.map_err(crate::errors::from_arrow)))
    }

    /// Writes the record batch of an `arrow_schema` and an `arrow_array` capsule.
    fn write_batch(&self, py: Python<'_>, schema: &Bound<'_, PyAny>, array: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = import_batch(schema, array)?;
        self.write_batches(py, std::iter::once(Ok(batch)))
    }

    /// Ends the current row group.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        self.with_open(py, |w| w.flush().map_err(from_pcodec))
    }

    /// The codec of every leaf column, once chosen.
    #[getter]
    fn codecs(&self, py: Python<'_>) -> PyResult<Option<Codecs>> {
        locked(py, &self.state, |state| {
            Ok(match state {
                State::Open(w) => w.codecs().map(|c| codec_map(&c)),
                State::Closed(c) => c.clone(),
            })
        })
    }

    /// Writes the footer; returns the codec of every leaf column.
    fn close(&self, py: Python<'_>) -> PyResult<Codecs> {
        locked(py, &self.state, |state| {
            let State::Open(w) = std::mem::replace(state, State::Closed(None)) else { return Err(closed()) };
            let (_, codecs) = w.finish().map_err(from_pcodec)?;
            *state = State::Closed(Some(codec_map(&codecs)));
            Ok(codec_map(&codecs))
        })
        .map_err(|e| self.pending.or(e))
    }

    /// Drops the writer without writing the footer.
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        locked(py, &self.state, |state| {
            *state = State::Closed(None);
            Ok(())
        })
    }

    #[getter]
    fn closed(&self, py: Python<'_>) -> PyResult<bool> {
        locked(py, &self.state, |state| Ok(matches!(state, State::Closed(_))))
    }
}

fn closed() -> PyErr {
    invalid("the writer is closed")
}

/// Writes every batch of an `arrow_array_stream` capsule to `sink` (a `str` path or a binary
/// file-like object); returns the codec of every leaf column.
#[pyfunction]
pub fn write_stream(
    py: Python<'_>,
    sink: &Bound<'_, PyAny>,
    stream: &Bound<'_, PyAny>,
    options: Options,
) -> PyResult<Codecs> {
    let reader = import_stream(stream)?;
    let w = Writer::create(py, sink, arrow_array::RecordBatchReader::schema(&reader), &options)?;
    w.write_batches(py, reader.map(|b| b.map_err(crate::errors::from_arrow)))?;
    w.close(py)
}

/// Leaf path to codec name, from the first row group (empty without row groups).
fn first_row_group_codecs(metadata: &ParquetMetaData) -> Codecs {
    metadata.row_groups().first().map_or_else(Codecs::new, |rg| {
        rg.columns().iter().map(|c| (c.column_path().string(), codec_name(c.compression()))).collect()
    })
}

/// Rewrites `input` to `sink`, with PCODEC (`target="pcodec"`) or as standard Parquet
/// (`target="standard"`, where PCODEC chunks get the base codec).
#[pyfunction]
pub fn transcode(
    py: Python<'_>,
    input: &File,
    sink: &Bound<'_, PyAny>,
    target: &str,
    options: Options,
) -> PyResult<Codecs> {
    let target = match target {
        "pcodec" => Target::Pcodec(options.writer_options()?),
        "standard" => Target::Standard { base: base_codec(&options.base)? },
        t => return Err(invalid(format!("unknown target {t:?}: expected \"pcodec\" or \"standard\""))),
    };
    let pending = Pending::default();
    let mut sink = Sink::new(sink, &pending)?;
    let reader = input.reader()?;
    detached(py, move || {
        let metadata = parquet_pcodec::transcode(reader, &mut sink, &target).map_err(from_pcodec)?;
        std::io::Write::flush(&mut sink).map_err(|e| invalid(e.to_string()))?;
        Ok(first_row_group_codecs(&metadata))
    })
    .map_err(|e| pending.or(e))
}
