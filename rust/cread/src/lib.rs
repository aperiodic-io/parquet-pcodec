//! C reading API over the tparquet reader; the contract is `include/tparquet_read.h`.
//!
//! Files open with [`tparquet::read`] (the parquet fork, so PCODEC pages decode), and their rows
//! leave as an Arrow C stream whose callbacks are implemented here: every one of them runs under
//! `catch_unwind`, so no panic of the reader, of the codec or of the Arrow export unwinds into C.
#![allow(clippy::missing_safety_doc)] // the header documents the contract

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, StructArray};
use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, Schema, SchemaRef};
use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fs::File;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tparquet::parquet::arrow::ProjectionMask;
use tparquet::parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

pub const TPR_OK: i32 = 0;
pub const TPR_ERR_ARGUMENT: i32 = 1;
pub const TPR_ERR_IO: i32 = 2;
pub const TPR_ERR_PARQUET: i32 = 3;
pub const TPR_ERR_PANIC: i32 = 4;
pub const TPR_DEFAULT_BATCH_SIZE: usize = 8192;

/// errno values the stream callbacks return (the C stream interface asks for errno codes)
const EIO: c_int = 5;
const EINVAL: c_int = 22;

const PANIC_MESSAGE: &str = "internal panic in tparquet_read";

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

/// An error: the code to return and its message.
type Failure = (i32, String);

fn arg(msg: impl Into<String>) -> Failure {
    (TPR_ERR_ARGUMENT, msg.into())
}

/// Opens `path` for reading; errors name the file.
fn open_file(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>, Failure> {
    tparquet::read::open(path).map_err(|e| {
        let code = if matches!(e, tparquet::Error::Io(_)) { TPR_ERR_IO } else { TPR_ERR_PARQUET };
        (code, format!("{}: {e}", path.display()))
    })
}

/// Runs `f` under `catch_unwind`: `ok` for a success, the error's code and message, or
/// `on_panic` for a panic.
fn catch<T>(ok: T, on_panic: T, f: impl FnOnce() -> Result<(), (T, String)>) -> (T, Option<String>) {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => (ok, None),
        Ok(Err((code, msg))) => (code, Some(msg)),
        Err(_) => (on_panic, Some(PANIC_MESSAGE.to_string())),
    }
}

/// Runs a `tpr_` function body: records its error message for `tpr_last_error` (or clears it).
fn guarded(f: impl FnOnce() -> Result<(), Failure>) -> i32 {
    let (code, msg) = catch(TPR_OK, TPR_ERR_PANIC, f);
    LAST_ERROR.with(|e| *e.borrow_mut() = msg.unwrap_or_default());
    code
}

fn path_arg(path: *const c_char) -> Result<PathBuf, Failure> {
    if path.is_null() {
        return Err(arg("NULL `path`"));
    }
    // SAFETY: the caller passes a NUL-terminated string (header contract)
    let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
    }
    #[cfg(not(unix))]
    {
        std::str::from_utf8(bytes).map(PathBuf::from).map_err(|_| arg("`path` is not UTF-8"))
    }
}

/// The names of `columns`, `n` NUL-terminated UTF-8 strings (NULL for all columns).
fn column_args(columns: *const *const c_char, n: usize) -> Result<Option<Vec<String>>, Failure> {
    if columns.is_null() {
        return if n == 0 { Ok(None) } else { Err(arg("NULL `columns` with n_columns > 0")) };
    }
    // SAFETY: the caller passes `n` pointers (header contract)
    let ptrs = unsafe { std::slice::from_raw_parts(columns, n) };
    ptrs.iter()
        .map(|p| {
            if p.is_null() {
                return Err(arg("NULL column name"));
            }
            // SAFETY: each name is NUL-terminated (header contract)
            let name = unsafe { CStr::from_ptr(*p) };
            name.to_str().map(str::to_string).map_err(|_| arg("column name is not UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// The root indices of `columns` in `schema`, in request order.
fn roots(schema: &Schema, columns: &[String]) -> Result<Vec<usize>, Failure> {
    let mut indices = Vec::with_capacity(columns.len());
    for name in columns {
        let i = schema.index_of(name).map_err(|_| arg(format!("no column named '{name}'")))?;
        if indices.contains(&i) {
            return Err(arg(format!("column '{name}' is requested more than once")));
        }
        indices.push(i);
    }
    Ok(indices)
}

/// A reader of the projected columns in the requested order, with the file's schema (and its
/// metadata) projected: the parquet reader yields roots in file order, without that metadata.
struct Reader {
    inner: ParquetRecordBatchReader,
    order: Vec<usize>,
    schema: SchemaRef,
}

impl Reader {
    fn open(path: PathBuf, columns: Option<Vec<String>>, batch_size: usize) -> Result<Self, Failure> {
        let builder = open_file(&path)?;
        let file_schema = builder.schema().clone();
        let wanted = match columns {
            Some(columns) => roots(&file_schema, &columns)?,
            None => (0..file_schema.fields().len()).collect(),
        };
        let mut sorted = wanted.clone();
        sorted.sort_unstable();
        let order: Vec<usize> = wanted.iter().map(|i| sorted.binary_search(i).unwrap_or_default()).collect();
        let schema = file_schema
            .project(&sorted)
            .and_then(|s| s.project(&order))
            .map_err(|e| (TPR_ERR_PARQUET, e.to_string()))?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), sorted);
        let inner = builder
            .with_projection(mask)
            .with_batch_size(batch_size)
            .build()
            .map_err(|e| (TPR_ERR_PARQUET, e.to_string()))?;
        Ok(Self { inner, order, schema: Arc::new(schema) })
    }

    /// The next batch, with the stream's schema (and so its metadata).
    fn next_batch(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        let batch = self.inner.next()?;
        Some(batch.and_then(|b| b.project(&self.order)).and_then(|b| b.with_schema(self.schema.clone())))
    }
}

/// What a stream's `private_data` owns.
struct Private {
    reader: Reader,
    last_error: Option<CString>,
}

/// Runs a stream callback body on the stream's private data: an error (or a panic) is recorded
/// for `get_last_error`, a success clears it.
unsafe fn stream_call(
    stream: *mut FFI_ArrowArrayStream,
    f: impl FnOnce(&mut Reader) -> Result<(), (c_int, String)>,
) -> c_int {
    if stream.is_null() {
        return EINVAL;
    }
    // SAFETY: a live stream's private_data is the `Private` that `tpr_open` boxed
    let private = unsafe { (*stream).private_data().cast::<Private>().as_mut() };
    let Some(private) = private else { return EINVAL };
    let (code, msg) = catch(0, EIO, || f(&mut private.reader));
    private.last_error = msg.map(|m| CString::new(m.replace('\0', " ")).unwrap_or_default());
    code
}

unsafe extern "C" fn get_schema(stream: *mut FFI_ArrowArrayStream, out: *mut FFI_ArrowSchema) -> c_int {
    unsafe {
        stream_call(stream, |reader| {
            if out.is_null() {
                return Err((EINVAL, "NULL `out`".into()));
            }
            let schema = FFI_ArrowSchema::try_from(reader.schema.as_ref()).map_err(|e| (EIO, e.to_string()))?;
            // SAFETY: `out` is writable; the consumer owns what it held before (released or empty)
            std::ptr::write(out, schema);
            Ok(())
        })
    }
}

unsafe extern "C" fn get_next(stream: *mut FFI_ArrowArrayStream, out: *mut FFI_ArrowArray) -> c_int {
    unsafe {
        stream_call(stream, |reader| {
            if out.is_null() {
                return Err((EINVAL, "NULL `out`".into()));
            }
            let array = match reader.next_batch() {
                None => FFI_ArrowArray::empty(),
                Some(Ok(batch)) => FFI_ArrowArray::new(&StructArray::from(batch).into()),
                Some(Err(e)) => {
                    std::ptr::write(out, FFI_ArrowArray::empty());
                    return Err((EIO, e.to_string()));
                }
            };
            // SAFETY: as in get_schema
            std::ptr::write(out, array);
            Ok(())
        })
    }
}

unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    // SAFETY: as in stream_call
    let private = unsafe { stream.as_ref().and_then(|s| s.private_data().cast::<Private>().as_ref()) };
    match private.and_then(|p| p.last_error.as_ref()) {
        Some(msg) => msg.as_ptr(),
        None => std::ptr::null(),
    }
}

unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    // SAFETY: as in stream_call; release runs once, then marks the stream released
    let Some(stream) = (unsafe { stream.as_mut() }) else { return };
    let private = stream.private_data().cast::<Private>();
    // dropping the reader closes the file; nothing in it panics, and if something did, the
    // stream is still marked released
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !private.is_null() {
            drop(unsafe { Box::from_raw(private) });
        }
    }));
    unsafe {
        stream.set_private_data(std::ptr::null_mut());
        stream.set_release(None);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn tpr_abi_version() -> u32 {
    1
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn tpr_open(
    path: *const c_char,
    columns: *const *const c_char,
    n_columns: usize,
    batch_size: i64,
    out: *mut FFI_ArrowArrayStream,
) -> i32 {
    guarded(|| {
        let path = path_arg(path)?;
        if out.is_null() {
            return Err(arg("NULL `out`"));
        }
        let columns = column_args(columns, n_columns)?;
        let batch_size = match batch_size {
            0 => TPR_DEFAULT_BATCH_SIZE,
            n => usize::try_from(n).map_err(|_| arg(format!("batch_size {n} is negative")))?,
        };
        let reader = Reader::open(path, columns, batch_size)?;
        let private = Box::into_raw(Box::new(Private { reader, last_error: None }));
        // SAFETY: the callbacks implement the C stream interface over `private`, which
        // `release` frees exactly once
        let stream = unsafe {
            FFI_ArrowArrayStream::new_unchecked(
                Some(get_schema),
                Some(get_next),
                Some(get_last_error),
                Some(release),
                private.cast::<c_void>(),
            )
        };
        // SAFETY: checked non-NULL; whatever `out` held is the caller's (not dropped here)
        unsafe { std::ptr::write(out, stream) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn tpr_file_info(path: *const c_char, num_rows: *mut i64, num_row_groups: *mut i64) -> i32 {
    guarded(|| {
        let builder = open_file(&path_arg(path)?)?;
        let meta = builder.metadata().file_metadata();
        // SAFETY: each output is NULL or writable (header contract)
        unsafe {
            if let Some(rows) = num_rows.as_mut() {
                *rows = meta.num_rows();
            }
            if let Some(groups) = num_row_groups.as_mut() {
                *groups = builder.metadata().num_row_groups() as i64;
            }
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn tpr_file_schema(path: *const c_char, out: *mut FFI_ArrowSchema) -> i32 {
    guarded(|| {
        let path = path_arg(path)?;
        if out.is_null() {
            return Err(arg("NULL `out`"));
        }
        let builder = open_file(&path)?;
        let schema =
            FFI_ArrowSchema::try_from(builder.schema().as_ref()).map_err(|e| (TPR_ERR_PARQUET, e.to_string()))?;
        // SAFETY: checked non-NULL
        unsafe { std::ptr::write(out, schema) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn tpr_last_error(buf: *mut c_char, cap: usize) -> usize {
    LAST_ERROR.with(|e| {
        let msg = e.borrow();
        if !buf.is_null() && cap > 0 {
            let n = msg.len().min(cap - 1);
            // SAFETY: the caller guarantees `buf` has `cap` writable bytes; n + 1 <= cap
            unsafe {
                std::ptr::copy_nonoverlapping(msg.as_ptr().cast::<c_char>(), buf, n);
                *buf.add(n) = 0;
            }
        }
        msg.len()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_is_an_error_not_an_unwind() {
        let code = guarded(|| panic!("boom"));
        assert_eq!(code, TPR_ERR_PANIC);
        LAST_ERROR.with(|e| assert_eq!(*e.borrow(), PANIC_MESSAGE));
        assert_eq!(guarded(|| Ok(())), TPR_OK);
        LAST_ERROR.with(|e| assert_eq!(*e.borrow(), ""));
        assert_eq!(guarded(|| Err((TPR_ERR_IO, "x".into()))), TPR_ERR_IO);
        LAST_ERROR.with(|e| assert_eq!(*e.borrow(), "x"));
    }

    #[test]
    fn a_panic_in_a_stream_callback_is_eio() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/files/mixed.parquet");
        let reader = Reader::open(path, None, 10).unwrap();
        let private = Box::into_raw(Box::new(Private { reader, last_error: None }));
        let mut stream = unsafe {
            FFI_ArrowArrayStream::new_unchecked(None, None, Some(get_last_error), Some(release), private.cast())
        };
        let code = unsafe { stream_call(&mut stream, |_| panic!("boom")) };
        assert_eq!(code, EIO);
        let msg = unsafe { CStr::from_ptr(get_last_error(&mut stream)) };
        assert_eq!(msg.to_str().unwrap(), PANIC_MESSAGE);
        // an error message with a NUL in it is still a C string
        let code = unsafe { stream_call(&mut stream, |_| Err((EINVAL, "a\0b".into()))) };
        assert_eq!(code, EINVAL);
        let msg = unsafe { CStr::from_ptr(get_last_error(&mut stream)) };
        assert_eq!(msg.to_str().unwrap(), "a b");
        assert_eq!(unsafe { stream_call(&mut stream, |_| Ok(())) }, 0);
        assert!(unsafe { get_last_error(&mut stream) }.is_null());
        drop(stream); // release frees the private data
    }

    #[test]
    fn a_stream_without_private_data_is_einval() {
        let mut stream =
            unsafe { FFI_ArrowArrayStream::new_unchecked(None, None, None, Some(release), std::ptr::null_mut()) };
        assert_eq!(unsafe { stream_call(&mut stream, |_| Ok(())) }, EINVAL);
        assert!(unsafe { get_last_error(&mut stream) }.is_null());
    }
}
