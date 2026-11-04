//! The Arrow PyCapsule interface: importing streams, arrays and schemas from any Python Arrow
//! library and exporting them, through arrow-rs's C data and C stream support.

use crate::errors::{PcodecError, from_arrow, invalid, locked, panic_message};
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
use arrow_array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CStr;
use std::sync::Mutex;

const STREAM: &CStr = c"arrow_array_stream";
const SCHEMA: &CStr = c"arrow_schema";
const ARRAY: &CStr = c"arrow_array";

fn capsule<'a, 'py>(obj: &'a Bound<'py, PyAny>, name: &CStr) -> PyResult<&'a Bound<'py, PyCapsule>> {
    obj.cast::<PyCapsule>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!("expected a PyCapsule named {:?}", name.to_string_lossy()))
    })
}

/// Takes the stream out of an `arrow_array_stream` capsule (the capsule keeps a released one).
pub fn import_stream(obj: &Bound<'_, PyAny>) -> PyResult<RecordBatchStreamReader> {
    let ptr = capsule(obj, STREAM)?.pointer_checked(Some(STREAM))?;
    // SAFETY: a capsule named "arrow_array_stream" holds an `ArrowArrayStream`; `from_raw`
    // moves it out and marks the original released, as the PyCapsule protocol asks.
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(ptr.as_ptr().cast()) };
    RecordBatchStreamReader::try_new(stream).map_err(from_arrow)
}

/// Reads the schema of an `arrow_schema` capsule.
pub fn import_schema(obj: &Bound<'_, PyAny>) -> PyResult<Schema> {
    let ptr = capsule(obj, SCHEMA)?.pointer_checked(Some(SCHEMA))?;
    // SAFETY: a capsule named "arrow_schema" holds an `ArrowSchema`, borrowed here.
    let schema = unsafe { &*ptr.as_ptr().cast::<FFI_ArrowSchema>() };
    Schema::try_from(schema).map_err(from_arrow)
}

/// The record batch of a pair of `arrow_schema` and `arrow_array` capsules holding a struct
/// array (`__arrow_c_array__` of a record batch).
pub fn import_batch(schema: &Bound<'_, PyAny>, array: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let sptr = capsule(schema, SCHEMA)?.pointer_checked(Some(SCHEMA))?;
    let aptr = capsule(array, ARRAY)?.pointer_checked(Some(ARRAY))?;
    // SAFETY: the capsules hold an `ArrowSchema` (borrowed) and an `ArrowArray`, which is
    // moved out: `from_raw` leaves the capsule's copy released.
    let data = unsafe {
        let schema = &*sptr.as_ptr().cast::<FFI_ArrowSchema>();
        let array = FFI_ArrowArray::from_raw(aptr.as_ptr().cast());
        from_ffi(array, schema)
    }
    .map_err(from_arrow)?;
    let array = arrow_array::make_array(data);
    let Some(s) = array.as_any().downcast_ref::<StructArray>() else {
        return Err(invalid(format!("expected a struct array (a record batch), got {}", array.data_type())));
    };
    Ok(RecordBatch::from(s.clone()))
}

fn export_schema<'py>(py: Python<'py>, schema: &Schema) -> PyResult<Bound<'py, PyCapsule>> {
    let ffi = FFI_ArrowSchema::try_from(schema).map_err(from_arrow)?;
    PyCapsule::new_with_value(py, ffi, SCHEMA)
}

/// A reader that releases the GIL while it decodes and reports panics as errors (a panic
/// must not unwind through the C stream callbacks).
struct Detached(Box<dyn RecordBatchReader + Send>);

impl Iterator for Detached {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let inner = &mut self.0;
        let mut next = move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inner.next()))
                .unwrap_or_else(|p| Some(Err(ArrowError::ExternalError(panic_message(&*p).into()))))
        };
        // the consumer may call from a thread that holds the GIL or from one that does not
        match Python::try_attach(|py| py.detach(&mut next)) {
            Some(r) => r,
            None => next(),
        }
    }
}

impl RecordBatchReader for Detached {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

fn consumed() -> PyErr {
    invalid("the stream was already consumed")
}

/// A stream of record batches, exported once through `__arrow_c_stream__`.
#[pyclass(frozen, module = "parquet_pcodec._lib")]
pub struct RecordBatchStream {
    schema: SchemaRef,
    reader: Mutex<Option<Box<dyn RecordBatchReader + Send>>>,
}

impl RecordBatchStream {
    pub fn new(reader: Box<dyn RecordBatchReader + Send>) -> Self {
        Self { schema: reader.schema(), reader: Mutex::new(Some(reader)) }
    }
}

#[pymethods]
impl RecordBatchStream {
    /// Exports the stream (once) as an `arrow_array_stream` capsule.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        if let Some(requested) = requested_schema {
            let requested = import_schema(&requested)?;
            if requested.fields() != self.schema.fields() {
                return Err(PcodecError::new_err(format!(
                    "cannot cast the stream to the requested schema: the file has {} and {} was requested",
                    self.schema, requested
                )));
            }
        }
        let reader = locked(py, &self.reader, |r| r.take().ok_or_else(consumed))?;
        PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(Box::new(Detached(reader))), STREAM)
    }

    /// The stream's schema as an `arrow_schema` capsule.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        export_schema(py, &self.schema)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next batch, decoded with the GIL released.
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Batch>> {
        let next = locked(py, &self.reader, |r| match r {
            Some(r) => r.next().transpose().map_err(from_arrow),
            None => Err(consumed()),
        })?;
        Ok(next.map(|batch| Batch { batch, schema: self.schema.clone() }))
    }
}

/// One record batch, exported through `__arrow_c_array__`.
#[pyclass(frozen, module = "parquet_pcodec._lib")]
pub struct Batch {
    batch: RecordBatch,
    /// The stream's schema, with its metadata.
    schema: SchemaRef,
}

#[pymethods]
impl Batch {
    /// The batch as a struct array: an `arrow_schema` and an `arrow_array` capsule.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        let _ = requested_schema;
        let array = FFI_ArrowArray::new(&StructArray::from(self.batch.clone()).into_data());
        Ok((export_schema(py, &self.schema)?, PyCapsule::new_with_value(py, array, ARRAY)?))
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// An Arrow schema, exported through `__arrow_c_schema__`.
#[pyclass(frozen, module = "parquet_pcodec._lib")]
pub struct ArrowSchema(pub SchemaRef);

#[pymethods]
impl ArrowSchema {
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        export_schema(py, &self.0)
    }

    /// The top-level field names.
    #[getter]
    fn names(&self) -> Vec<String> {
        self.0.fields().iter().map(|f| f.name().clone()).collect()
    }
}
