//! The upstream `parquet` 60.1.0 crate does not know PCODEC: reading a PCODEC file fails with
//! an error naming the unknown codec, and it never returns data. The standard control file
//! still reads.

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::{Path, PathBuf};

fn testdata(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/files").join(format!("{name}.parquet"))
}

const PCODEC_FILES: [&str; 12] = [
    "primitives_required",
    "primitives_optional",
    "lists",
    "nested_struct",
    "temporal",
    "decimal_floats",
    "float_specials",
    "empty",
    "multiple_row_groups",
    "pages_v1",
    "pages_v2",
    "mixed",
];

#[test]
fn upstream_rejects_every_pcodec_file() {
    for name in PCODEC_FILES {
        let err = SerializedFileReader::new(File::open(testdata(name)).unwrap()).err();
        let err = err.unwrap_or_else(|| panic!("{name}: upstream read a PCODEC file"));
        assert!(err.to_string().contains("Unexpected CompressionCodec 20547"), "{name}: {err}");
        let err = ParquetRecordBatchReaderBuilder::try_new(File::open(testdata(name)).unwrap()).err();
        let err = err.unwrap_or_else(|| panic!("{name}: upstream read a PCODEC file"));
        assert!(err.to_string().contains("Unexpected CompressionCodec 20547"), "{name}: {err}");
    }
}

#[test]
fn upstream_reads_the_control_file() {
    let r = SerializedFileReader::new(File::open(testdata("standard_control")).unwrap()).unwrap();
    assert_eq!(r.metadata().file_metadata().num_rows(), 1500);
    let rows: usize = ParquetRecordBatchReaderBuilder::try_new(File::open(testdata("standard_control")).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(rows, 1500);
}
