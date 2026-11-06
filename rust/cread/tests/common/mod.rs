//! Calls the C API the way a C program does, and imports its streams with arrow-rs.
#![allow(dead_code)] // each test binary uses part of it

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::{FFI_ArrowArrayStream, RecordBatchStreamReader};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, Schema};
use std::ffi::{CStr, CString, c_char};
use std::path::{Path, PathBuf};
use tparquet_read::*;

pub fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata")
}

pub fn cpath(path: &Path) -> CString {
    CString::new(path.to_str().unwrap()).unwrap()
}

/// The calling thread's last error message.
pub fn last_error() -> String {
    let mut buf = [0 as c_char; 1024];
    let n = unsafe { tpr_last_error(buf.as_mut_ptr(), buf.len()) };
    let s = unsafe { CStr::from_ptr(buf.as_ptr()) }.to_str().unwrap().to_string();
    assert_eq!(n, s.len());
    s
}

/// `tpr_open` with `columns` (None for all); the raw stream, or the code and message.
pub fn open_raw(path: &Path, columns: Option<&[&str]>, batch_size: i64) -> Result<FFI_ArrowArrayStream, (i32, String)> {
    let path = cpath(path);
    let names: Vec<CString> = columns.unwrap_or(&[]).iter().map(|c| CString::new(*c).unwrap()).collect();
    let ptrs: Vec<*const c_char> = names.iter().map(|c| c.as_ptr()).collect();
    let (cols, n) = match columns {
        Some(_) => (ptrs.as_ptr(), ptrs.len()),
        None => (std::ptr::null(), 0),
    };
    let mut out = FFI_ArrowArrayStream::empty();
    let rc = unsafe { tpr_open(path.as_ptr(), cols, n, batch_size, &mut out) };
    if rc == TPR_OK {
        assert!(out.release().is_some(), "a stream after TPR_OK");
        assert_eq!(last_error(), "");
        Ok(out)
    } else {
        assert!(out.release().is_none(), "no stream after an error");
        Err((rc, last_error()))
    }
}

pub fn open(path: &Path, columns: Option<&[&str]>, batch_size: i64) -> Result<RecordBatchStreamReader, (i32, String)> {
    open_raw(path, columns, batch_size).map(|s| RecordBatchStreamReader::try_new(s).unwrap())
}

/// Every batch of the file (all columns, default batch size).
pub fn read_all(path: &Path) -> Result<Vec<RecordBatch>, ArrowError> {
    let reader = open(path, None, 0).unwrap();
    reader.collect()
}

pub fn file_schema(path: &Path) -> Result<Schema, (i32, String)> {
    let path = cpath(path);
    let mut out = FFI_ArrowSchema::empty();
    match unsafe { tpr_file_schema(path.as_ptr(), &mut out) } {
        TPR_OK => Ok(Schema::try_from(&out).unwrap()),
        rc => {
            assert!(out.release().is_none());
            Err((rc, last_error()))
        }
    }
}

pub fn file_info(path: &Path) -> Result<(i64, i64), (i32, String)> {
    let path = cpath(path);
    let (mut rows, mut groups) = (-1i64, -1i64);
    match unsafe { tpr_file_info(path.as_ptr(), &mut rows, &mut groups) } {
        TPR_OK => Ok((rows, groups)),
        rc => {
            assert_eq!((rows, groups), (-1, -1), "outputs untouched on error");
            Err((rc, last_error()))
        }
    }
}

/// The canonical JSON lines (testdata/README.md) of `batches`, `\n`-terminated.
pub fn jsonl(batches: &[RecordBatch]) -> String {
    let mut out = String::new();
    for b in batches {
        for line in tparquet::canonical::batch_lines(b).unwrap() {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out
}

/// Writes `batches` with the tparquet writer (`pcodec`: every eligible column PCODEC, else
/// the base codec) and returns the file's bytes.
pub fn write(batches: &[RecordBatch], pcodec: bool, row_group_rows: usize) -> Vec<u8> {
    use tparquet::parquet::file::properties::WriterProperties;
    use tparquet::{Policy, Writer, WriterOptions};
    let options = WriterOptions::default()
        .with_policy(if pcodec { Policy::Pcodec } else { Policy::Base })
        .with_properties(WriterProperties::builder().set_max_row_group_row_count(Some(row_group_rows)).build());
    let mut buf = Vec::new();
    let mut w = Writer::try_new(&mut buf, batches[0].schema(), options).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
    buf
}
