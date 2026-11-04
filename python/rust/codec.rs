//! The PCODEC page payload on its own (spec section 4), over `parquet-pcodec-core`.

use crate::errors::{detached, invalid};
use parquet_pcodec_core::{Mode, Options, PhysicalType};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyBytes;

fn hint(name: &str) -> PyResult<PhysicalType> {
    match name {
        "INT32" => Ok(PhysicalType::Int32),
        "INT64" => Ok(PhysicalType::Int64),
        "FLOAT" => Ok(PhysicalType::Float),
        "DOUBLE" => Ok(PhysicalType::Double),
        _ => Err(invalid(format!("unknown hint {name:?}: expected INT32, INT64, FLOAT or DOUBLE"))),
    }
}

/// Encodes `data` as a payload, with the column's physical type as `hint`.
#[pyfunction]
#[pyo3(signature = (data, hint=None, level=8))]
pub fn compress<'py>(
    py: Python<'py>,
    data: PyBackedBytes,
    hint: Option<&str>,
    level: i64,
) -> PyResult<Bound<'py, PyBytes>> {
    let level = u8::try_from(level).map_err(|_| invalid(format!("compression level {level} is not in 0..=12")))?;
    let opts = Options { level, hint: hint.map(self::hint).transpose()?, ..Options::default() };
    let payload = detached(py, || parquet_pcodec_core::compress(&data, &opts).map_err(|e| invalid(e.to_string())))?;
    Ok(PyBytes::new(py, &payload))
}

/// Decodes a payload that must produce exactly `expected_len` bytes.
#[pyfunction]
pub fn decompress<'py>(py: Python<'py>, payload: PyBackedBytes, expected_len: i64) -> PyResult<Bound<'py, PyBytes>> {
    let n = usize::try_from(expected_len)
        .map_err(|_| invalid(format!("expected_len must not be negative, got {expected_len}")))?;
    let data = detached(py, || parquet_pcodec_core::decompress(&payload, n).map_err(|e| invalid(e.to_string())))?;
    Ok(PyBytes::new(py, &data))
}

/// The mode of a payload: `RAW`, `PCO`, `DECIMAL` or `ZSTD`.
#[pyfunction]
pub fn mode(payload: &[u8]) -> PyResult<&'static str> {
    match parquet_pcodec_core::mode_of(payload).map_err(|e| invalid(e.to_string()))? {
        Mode::Raw => Ok("RAW"),
        Mode::Pco => Ok("PCO"),
        Mode::Decimal => Ok("DECIMAL"),
        // `Mode` is non-exhaustive: every mode `mode_of` accepts is listed above
        _ => Ok("ZSTD"),
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compress, m)?)?;
    m.add_function(wrap_pyfunction!(decompress, m)?)?;
    m.add_function(wrap_pyfunction!(mode, m)?)?;
    m.add("CODEC_ID", parquet_pcodec_core::CODEC_ID)?;
    m.add("FORMAT_VERSION", parquet_pcodec_core::FORMAT_VERSION)?;
    m.add("KV_KEY", parquet_pcodec_core::KV_KEY)?;
    Ok(())
}
