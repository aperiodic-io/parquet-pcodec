//! The C API contract (`include/tparquet_read.h`): arguments, projection, errors, and the Arrow
//! C stream protocol.

mod common;

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema};
use common::*;
use std::ffi::{CStr, CString, c_char};
use std::path::Path;
use std::sync::Arc;
use tparquet_read::*;

fn mixed() -> std::path::PathBuf {
    testdata().join("files/mixed.parquet")
}

#[test]
fn constants() {
    assert_eq!(tpr_abi_version(), 1);
    assert_eq!(TPR_DEFAULT_BATCH_SIZE, 8192);
    assert_eq!((TPR_OK, TPR_ERR_ARGUMENT, TPR_ERR_IO, TPR_ERR_PARQUET, TPR_ERR_PANIC), (0, 1, 2, 3, 4));
}

#[test]
fn header_matches_the_constants() {
    let h = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("include/tparquet_read.h")).unwrap();
    for (name, value) in [
        ("TPR_ABI_VERSION", tpr_abi_version() as i64),
        ("TPR_OK", TPR_OK as i64),
        ("TPR_ERR_ARGUMENT", TPR_ERR_ARGUMENT as i64),
        ("TPR_ERR_IO", TPR_ERR_IO as i64),
        ("TPR_ERR_PARQUET", TPR_ERR_PARQUET as i64),
        ("TPR_ERR_PANIC", TPR_ERR_PANIC as i64),
        ("TPR_DEFAULT_BATCH_SIZE", TPR_DEFAULT_BATCH_SIZE as i64),
    ] {
        assert!(h.contains(&format!("#define {name} {value}")), "{name}");
    }
}

#[test]
fn default_batch_size() {
    let path = testdata().join("files/primitives_required.parquet"); // 3000 rows
    let sizes = |b: i64| -> Vec<usize> { open(&path, None, b).unwrap().map(|b| b.unwrap().num_rows()).collect() };
    assert_eq!(sizes(0), vec![3000]);
    assert_eq!(sizes(1000), vec![1000, 1000, 1000]);
    assert_eq!(sizes(1), vec![1; 3000]);
    // a batch size above the default is not capped to it
    let big: Vec<RecordBatch> = (0..3).map(|_| batch(10_000, 0)).collect();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("big.parquet");
    std::fs::write(&file, write(&big, true, 1 << 20)).unwrap();
    let sizes = |b: i64| -> Vec<usize> { open(&file, None, b).unwrap().map(|b| b.unwrap().num_rows()).collect() };
    assert_eq!(sizes(0), vec![8192, 8192, 8192, 5424]);
    assert_eq!(sizes(30_000), vec![30_000]);
    assert_eq!(open(&file, None, -5).unwrap_err().0, TPR_ERR_ARGUMENT);
    assert!(last_error().contains("batch_size"), "{}", last_error());
}

#[test]
fn projection_in_requested_order() {
    let all = read_all(&mixed()).unwrap();
    let full = &all[0];
    let reader = open(&mixed(), Some(&["note", "ts", "price"]), 0).unwrap();
    let schema = reader.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["note", "ts", "price"]);
    assert_eq!(schema.metadata().get("parquet.pcodec.version").map(String::as_str), Some("1"));
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let got = &batches[0];
    for (i, name) in names.iter().enumerate() {
        assert_eq!(got.column(i).as_ref(), full.column_by_name(name).unwrap().as_ref(), "{name}");
    }
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1500);
    // a single column, and no column at all
    let one: Vec<RecordBatch> = open(&mixed(), Some(&["qty"]), 0).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(one[0].num_columns(), 1);
    let none = open(&mixed(), Some(&[]), 0).unwrap();
    assert_eq!(none.schema().fields().len(), 0);
}

#[test]
fn projection_errors() {
    let (rc, msg) = open(&mixed(), Some(&["ts", "nope"]), 0).unwrap_err();
    assert_eq!(rc, TPR_ERR_ARGUMENT);
    assert_eq!(msg, "no column named 'nope'");
    let (rc, msg) = open(&mixed(), Some(&["ts", "price", "ts"]), 0).unwrap_err();
    assert_eq!(rc, TPR_ERR_ARGUMENT);
    assert_eq!(msg, "column 'ts' is requested more than once");
    // NULL columns with a count, a NULL name, a name that is not UTF-8
    let path = cpath(&mixed());
    let mut out = FFI_ArrowArrayStream::empty();
    assert_eq!(unsafe { tpr_open(path.as_ptr(), std::ptr::null(), 1, 0, &mut out) }, TPR_ERR_ARGUMENT);
    assert!(last_error().contains("NULL"), "{}", last_error());
    let names = [std::ptr::null::<c_char>()];
    assert_eq!(unsafe { tpr_open(path.as_ptr(), names.as_ptr(), 1, 0, &mut out) }, TPR_ERR_ARGUMENT);
    assert!(last_error().contains("NULL"), "{}", last_error());
    let bad = CString::new(vec![0xffu8, 0xfe]).unwrap();
    let names = [bad.as_ptr()];
    assert_eq!(unsafe { tpr_open(path.as_ptr(), names.as_ptr(), 1, 0, &mut out) }, TPR_ERR_ARGUMENT);
    assert!(last_error().contains("UTF-8"), "{}", last_error());
    assert!(out.release().is_none());
}

#[test]
fn file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.parquet");
    let (rc, msg) = open(&missing, None, 0).unwrap_err();
    assert_eq!(rc, TPR_ERR_IO);
    assert!(msg.contains("missing.parquet"), "{msg}");
    assert_eq!(file_info(&missing).unwrap_err().0, TPR_ERR_IO);
    assert_eq!(file_schema(&missing).unwrap_err().0, TPR_ERR_IO);

    let not_parquet = dir.path().join("x.parquet");
    std::fs::write(&not_parquet, b"this is not a parquet file at all").unwrap();
    let (rc, msg) = open(&not_parquet, None, 0).unwrap_err();
    assert_eq!(rc, TPR_ERR_PARQUET);
    assert!(!msg.is_empty());
    assert_eq!(file_info(&not_parquet).unwrap_err().0, TPR_ERR_PARQUET);
    assert_eq!(file_schema(&not_parquet).unwrap_err().0, TPR_ERR_PARQUET);

    // a directory opens (on Linux) but cannot be read: the parquet reader reports it
    let (rc, msg) = open(dir.path(), None, 0).unwrap_err();
    assert!(rc == TPR_ERR_IO || rc == TPR_ERR_PARQUET, "{rc}");
    assert!(msg.starts_with(dir.path().to_str().unwrap()), "{msg}");
}

#[test]
fn null_arguments() {
    let path = cpath(&mixed());
    let mut out = FFI_ArrowArrayStream::empty();
    assert_eq!(unsafe { tpr_open(std::ptr::null(), std::ptr::null(), 0, 0, &mut out) }, TPR_ERR_ARGUMENT);
    assert_eq!(last_error(), "NULL `path`");
    assert_eq!(unsafe { tpr_open(path.as_ptr(), std::ptr::null(), 0, 0, std::ptr::null_mut()) }, TPR_ERR_ARGUMENT);
    assert_eq!(last_error(), "NULL `out`");
    let mut schema = FFI_ArrowSchema::empty();
    assert_eq!(unsafe { tpr_file_schema(std::ptr::null(), &mut schema) }, TPR_ERR_ARGUMENT);
    assert_eq!(unsafe { tpr_file_schema(path.as_ptr(), std::ptr::null_mut()) }, TPR_ERR_ARGUMENT);
    assert_eq!(last_error(), "NULL `out`");
    let mut n = 0i64;
    assert_eq!(unsafe { tpr_file_info(std::ptr::null(), &mut n, &mut n) }, TPR_ERR_ARGUMENT);
    assert_eq!(last_error(), "NULL `path`");
    // either output of tpr_file_info may be NULL
    let mut rows = 0i64;
    assert_eq!(unsafe { tpr_file_info(path.as_ptr(), &mut rows, std::ptr::null_mut()) }, TPR_OK);
    assert_eq!(rows, 1500);
    let mut groups = 0i64;
    assert_eq!(unsafe { tpr_file_info(path.as_ptr(), std::ptr::null_mut(), &mut groups) }, TPR_OK);
    assert_eq!(groups, 1);
    assert_eq!(unsafe { tpr_file_info(path.as_ptr(), std::ptr::null_mut(), std::ptr::null_mut()) }, TPR_OK);
}

#[test]
fn last_error_copies_and_truncates() {
    let (_, msg) = open(Path::new("/nonexistent/dir/file.parquet"), None, 0).unwrap_err();
    assert!(msg.len() > 8);
    let mut buf = [b'x' as c_char; 6];
    let n = unsafe { tpr_last_error(buf.as_mut_ptr(), buf.len()) };
    assert_eq!(n, msg.len());
    assert_eq!(unsafe { CStr::from_ptr(buf.as_ptr()) }.to_str().unwrap(), &msg[..5]);
    // NULL buffer or no room: only the length
    assert_eq!(unsafe { tpr_last_error(std::ptr::null_mut(), 100) }, msg.len());
    let mut one = [b'x' as c_char; 1];
    assert_eq!(unsafe { tpr_last_error(one.as_mut_ptr(), 0) }, msg.len());
    assert_eq!(one[0], b'x' as c_char);
    assert_eq!(unsafe { tpr_last_error(one.as_mut_ptr(), 1) }, msg.len());
    assert_eq!(one[0], 0);
    // a success clears it
    file_info(&mixed()).unwrap();
    assert_eq!(unsafe { tpr_last_error(std::ptr::null_mut(), 0) }, 0);
}

#[test]
fn last_error_is_per_thread() {
    let _ = open(Path::new("/nonexistent.parquet"), None, 0).unwrap_err();
    std::thread::spawn(|| assert_eq!(last_error(), "")).join().unwrap();
    assert!(!last_error().is_empty());
}

/// The stream callbacks, called directly as a C consumer calls them.
#[test]
fn stream_protocol() {
    let mut stream = open_raw(&testdata().join("files/multiple_row_groups.parquet"), None, 1000).unwrap();
    let get_schema = stream.get_schema().unwrap();
    let get_next = stream.get_next().unwrap();
    let get_last_error = stream.get_last_error().unwrap();
    let release = stream.release().unwrap();
    assert!(unsafe { get_last_error(&mut stream) }.is_null());
    // the schema can be asked for more than once, before and after reading
    for _ in 0..2 {
        let mut schema = FFI_ArrowSchema::empty();
        assert_eq!(unsafe { get_schema(&mut stream, &mut schema) }, 0);
        assert!(schema.release().is_some());
        assert_eq!(
            Schema::try_from(&schema).unwrap(),
            file_schema(&testdata().join("files/multiple_row_groups.parquet")).unwrap()
        );
    }
    let mut rows = 0;
    loop {
        let mut array = FFI_ArrowArray::empty();
        assert_eq!(unsafe { get_next(&mut stream, &mut array) }, 0);
        if array.is_released() {
            break;
        }
        rows += array.len();
        assert!(array.len() <= 1000);
    }
    assert_eq!(rows, 2000);
    // after the end, the end again
    let mut array = FFI_ArrowArray::empty();
    assert_eq!(unsafe { get_next(&mut stream, &mut array) }, 0);
    assert!(array.is_released());
    let mut schema = FFI_ArrowSchema::empty();
    assert_eq!(unsafe { get_schema(&mut stream, &mut schema) }, 0);
    assert!(unsafe { get_last_error(&mut stream) }.is_null());
    unsafe { release(&mut stream) };
    assert!(stream.release().is_none(), "release marks the stream released");
    assert!(stream.private_data().is_null());
}

#[test]
fn stream_callbacks_reject_null_outputs() {
    let mut stream = open_raw(&mixed(), None, 0).unwrap();
    let get_schema = stream.get_schema().unwrap();
    let get_next = stream.get_next().unwrap();
    let get_last_error = stream.get_last_error().unwrap();
    assert_eq!(unsafe { get_schema(&mut stream, std::ptr::null_mut()) }, libc_einval());
    let msg = unsafe { CStr::from_ptr(get_last_error(&mut stream)) }.to_str().unwrap().to_string();
    assert_eq!(msg, "NULL `out`");
    assert_eq!(unsafe { get_next(&mut stream, std::ptr::null_mut()) }, libc_einval());
    // a NULL stream
    assert_eq!(unsafe { get_schema(std::ptr::null_mut(), std::ptr::null_mut()) }, libc_einval());
    assert_eq!(unsafe { get_next(std::ptr::null_mut(), std::ptr::null_mut()) }, libc_einval());
    assert!(unsafe { get_last_error(std::ptr::null_mut()) }.is_null());
    let release = stream.release().unwrap();
    unsafe { release(std::ptr::null_mut()) };
    // a success after an error clears it
    let mut schema = FFI_ArrowSchema::empty();
    assert_eq!(unsafe { get_schema(&mut stream, &mut schema) }, 0);
    assert!(unsafe { get_last_error(&mut stream) }.is_null());
}

fn libc_einval() -> i32 {
    22
}

fn batch(rows: usize, seed: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("price", DataType::Float64, true),
        Field::new("sym", DataType::Utf8, true),
    ]));
    let ts = Int64Array::from_iter_values((0..rows as i64).map(|i| 1_700_000_000_000 + seed * 1_000_000 + i * 7));
    let price = Float64Array::from_iter((0..rows).map(|i| (i % 17 != 0).then(|| (10_000 + (i % 300)) as f64 / 100.0)));
    let sym = StringArray::from_iter((0..rows).map(|i| (i % 5 != 0).then(|| ["AAPL", "MSFT", "GOOG"][i % 3])));
    RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(price), Arc::new(sym)]).unwrap()
}

/// The byte ranges of the file's PCODEC column chunks.
fn pcodec_chunks(file: &[u8]) -> Vec<(usize, usize)> {
    use tparquet::parquet::basic::Compression;
    use tparquet::parquet::file::reader::{FileReader, SerializedFileReader};
    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(file)).unwrap();
    let mut out = vec![];
    for rg in reader.metadata().row_groups() {
        for c in rg.columns() {
            if matches!(c.compression(), Compression::PCODEC(_)) {
                let (start, len) = c.byte_range();
                out.push((start as usize, len as usize));
            }
        }
    }
    out
}

#[test]
fn a_corrupt_page_is_an_error_from_get_next() {
    let file = write(&[batch(5000, 0)], true, 1 << 20);
    let chunks = pcodec_chunks(&file);
    assert_eq!(chunks.len(), 2, "ts and price");
    let mut bad = file.clone();
    // the second half of each chunk: payload bytes, after the page header
    for (start, len) in chunks {
        for b in &mut bad[start + len / 2..start + len] {
            *b = 0xff;
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.parquet");
    std::fs::write(&path, &bad).unwrap();
    let (rows, _) = file_info(&path).unwrap(); // the footer is intact
    assert_eq!(rows, 5000);
    let mut stream = open_raw(&path, None, 0).unwrap();
    let get_next = stream.get_next().unwrap();
    let get_last_error = stream.get_last_error().unwrap();
    let mut array = FFI_ArrowArray::empty();
    assert_eq!(unsafe { get_next(&mut stream, &mut array) }, 5, "EIO");
    assert!(array.is_released(), "no array on error");
    let msg = unsafe { CStr::from_ptr(get_last_error(&mut stream)) }.to_str().unwrap().to_string();
    assert!(!msg.is_empty());
    // the error is also what an arrow-rs consumer sees
    let err = read_all(&path).unwrap_err();
    assert!(err.to_string().contains(&msg) || !err.to_string().is_empty(), "{err}");
}

#[test]
fn reads_what_the_writer_wrote() {
    let batches = [batch(3000, 0), batch(2500, 1), batch(1, 2)];
    for pcodec in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        std::fs::write(&path, write(&batches, pcodec, 2000)).unwrap();
        assert_eq!(file_info(&path).unwrap(), (5501, 3));
        assert_eq!(jsonl(&read_all(&path).unwrap()), jsonl(&batches));
        let schema = file_schema(&path).unwrap();
        let version = schema.metadata().get("parquet.pcodec.version").map(String::as_str);
        assert_eq!(version, pcodec.then_some("1"));
        assert_eq!(schema.fields(), batches[0].schema().fields());
        let nulls: usize = read_all(&path).unwrap().iter().map(|b| b.column(1).null_count()).sum();
        assert_eq!(nulls, batches.iter().map(|b| b.column(1).null_count()).sum::<usize>());
    }
}
