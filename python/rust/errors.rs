//! Rust errors as Python exceptions; panics stop here.

use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use std::any::Any;
use std::path::Path;
use std::sync::{Arc, Mutex};

pyo3::create_exception!(
    _lib,
    PcodecError,
    PyValueError,
    "A Parquet file, a PCODEC payload or an argument is not valid."
);

/// A `PcodecError` with `msg`.
pub fn invalid(msg: impl Into<String>) -> PyErr {
    PcodecError::new_err(msg.into())
}

/// An `OSError` (the subclass Python picks from the errno, such as `FileNotFoundError`) for
/// `path`.
pub fn os_error(e: &std::io::Error, path: &Path) -> PyErr {
    let path = path.display().to_string();
    match e.raw_os_error() {
        Some(errno) => PyOSError::new_err((errno, e.to_string(), path)),
        None => PyOSError::new_err(format!("{path}: {e}")),
    }
}

/// The message of a panic payload.
pub fn panic_message(panic: &(dyn Any + Send)) -> String {
    let msg = panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    format!("internal error: {msg}")
}

/// Runs `f` with the GIL released, turning a panic into a `PcodecError`.
pub fn detached<T: Send>(py: Python<'_>, f: impl FnOnce() -> PyResult<T> + Send) -> PyResult<T> {
    py.detach(|| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or_else(|p| Err(invalid(panic_message(&*p))))
    })
}

/// Runs `f` on the value behind `m` with the GIL released, waiting for the lock without it:
/// a thread that holds the GIL must never wait for a lock that a thread without the GIL holds
/// (that thread may need the GIL to finish, through a Python file object).
pub fn locked<T: Send, R: Send>(
    py: Python<'_>,
    m: &Mutex<T>,
    f: impl FnOnce(&mut T) -> PyResult<R> + Send,
) -> PyResult<R> {
    detached(py, || f(&mut m.lock().unwrap_or_else(|p| p.into_inner())))
}

/// Where a sink backed by a Python object keeps the exception it raised, so that it reaches
/// the caller unchanged instead of as an I/O error message.
#[derive(Clone, Default)]
pub struct Pending(Arc<Mutex<Option<PyErr>>>);

impl Pending {
    pub fn set(&self, e: PyErr) {
        let mut slot = self.0.lock().unwrap_or_else(|p| p.into_inner());
        slot.get_or_insert(e);
    }

    /// The pending exception if there is one, else `e`.
    pub fn or(&self, e: PyErr) -> PyErr {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).take().unwrap_or(e)
    }
}

/// A `pparquet` error as a `PcodecError` (or the `OSError` it wraps).
pub fn from_pcodec(e: pparquet::Error) -> PyErr {
    match e {
        pparquet::Error::Io(io) => PyOSError::new_err(io.to_string()),
        e => invalid(e.to_string()),
    }
}

pub fn from_parquet(e: pparquet::parquet::errors::ParquetError) -> PyErr {
    invalid(format!("parquet: {e}"))
}

pub fn from_arrow(e: arrow_schema::ArrowError) -> PyErr {
    invalid(format!("arrow: {e}"))
}

/// Panics in Rust code called from Python, for the tests that check a panic becomes a
/// `PcodecError`: with a `String` payload (`message`), a `&str` one (`None`) or neither
/// (`""`, a payload that is not a string).
#[pyfunction]
#[pyo3(name = "_panic", signature = (message=None))]
pub fn panic_for_tests(py: Python<'_>, message: Option<String>) -> PyResult<()> {
    detached(py, move || match message.as_deref() {
        None => panic!("a static message"),
        Some("") => std::panic::panic_any(42_u8),
        Some(_) => std::panic::panic_any(message.unwrap_or_default()),
    })
}

/// A stream whose reader panics on its first batch, for the same tests.
#[pyclass(frozen, module = "pparquet._lib")]
pub struct PanickingStream;

struct Panics(arrow_schema::SchemaRef);

impl Iterator for Panics {
    type Item = Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        panic!("the reader panicked")
    }
}

impl arrow_array::RecordBatchReader for Panics {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.0.clone()
    }
}

#[pymethods]
impl PanickingStream {
    #[new]
    fn new() -> Self {
        Self
    }

    fn stream(&self) -> crate::ffi::RecordBatchStream {
        let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Int64,
            true,
        )]));
        crate::ffi::RecordBatchStream::new(Box::new(Panics(schema)))
    }
}
