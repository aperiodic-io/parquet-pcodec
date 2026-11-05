//! `tparquet._lib`: the extension module behind the `tparquet` Python package.
//!
//! Data crosses the boundary only through the Arrow PyCapsule interface, so the module needs
//! no Python Arrow library. The GIL is released while files are read, encoded and decoded.

mod codec;
mod errors;
mod ffi;
mod file;
mod options;
mod sink;
mod writer;

use pyo3::prelude::*;

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("PcodecError", m.py().get_type::<errors::PcodecError>())?;
    m.add_class::<ffi::RecordBatchStream>()?;
    m.add_class::<ffi::ArrowSchema>()?;
    m.add_class::<ffi::Batch>()?;
    m.add_class::<file::File>()?;
    m.add_class::<writer::Writer>()?;
    m.add_function(wrap_pyfunction!(file::open_path, m)?)?;
    m.add_function(wrap_pyfunction!(file::open_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(file::verify, m)?)?;
    m.add_function(wrap_pyfunction!(writer::transcode, m)?)?;
    m.add_function(wrap_pyfunction!(writer::write_stream, m)?)?;
    codec::register(m)?;
    m.add_function(wrap_pyfunction!(errors::panic_for_tests, m)?)?;
    m.add_class::<errors::PanickingStream>()?;
    Ok(())
}
