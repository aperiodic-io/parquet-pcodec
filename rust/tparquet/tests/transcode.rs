mod common;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use bytes::Bytes;
use common::{codecs, lines, report};
use std::collections::HashMap;
use std::sync::Arc;
use tparquet::parquet::arrow::ArrowWriter;
use tparquet::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tparquet::parquet::basic::{Compression, GzipLevel};
use tparquet::parquet::file::metadata::{KeyValue, SortingColumn};
use tparquet::parquet::file::properties::WriterProperties;
use tparquet::verify::verify;
use tparquet::{Policy, Target, WriterOptions, transcode};

fn batch(offset: i64, rows: i64) -> RecordBatch {
    let ids = Int64Array::from_iter_values((offset..offset + rows).map(|i| i * 3));
    let px = Float64Array::from_iter((offset..offset + rows).map(|i| (i % 9 != 0).then_some(i as f64 / 4.0)));
    let names = StringArray::from_iter_values((offset..offset + rows).map(|i| format!("s{}", i % 7)));
    let b = RecordBatch::try_from_iter([
        ("id", Arc::new(ids) as ArrayRef),
        ("px", Arc::new(px) as ArrayRef),
        ("name", Arc::new(names) as ArrayRef),
    ])
    .unwrap();
    let metadata = HashMap::from([("origin".to_string(), "test".to_string())]);
    let schema = Arc::new(Schema::new_with_metadata(b.schema().fields().clone(), metadata));
    b.with_schema(schema).unwrap()
}

/// A standard file: SNAPPY, row groups of 1000, 1000 and 500 rows, a custom key-value pair.
fn standard() -> Bytes {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_key_value_metadata(Some(vec![KeyValue::new("custom".to_string(), "kept".to_string())]))
        .build();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, batch(0, 1).schema(), Some(props)).unwrap();
    for (offset, rows) in [(0, 1000), (1000, 1000), (2000, 500)] {
        w.write(&batch(offset, rows)).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
    Bytes::from(out)
}

fn run(input: Bytes, target: &Target) -> Bytes {
    let mut out = Vec::new();
    transcode(input, &mut out, target).unwrap();
    Bytes::from(out)
}

fn row_groups(file: &Bytes) -> Vec<i64> {
    let b = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
    b.metadata().row_groups().iter().map(|rg| rg.num_rows()).collect()
}

fn kv(file: &Bytes) -> Vec<(String, Option<String>)> {
    let b = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
    let kv = b.metadata().file_metadata().key_value_metadata().cloned().unwrap_or_default();
    kv.into_iter().filter(|kv| kv.key != "ARROW:schema").map(|kv| (kv.key, kv.value)).collect()
}

/// The Arrow schema as written in `ARROW:schema` (the reader merges the other key-value
/// pairs into its metadata)
fn arrow_schema(file: &Bytes) -> Schema {
    let b = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
    let file = b.metadata().file_metadata();
    let kv = file.key_value_metadata().cloned().unwrap_or_default();
    let arrow: Vec<KeyValue> = kv.into_iter().filter(|kv| kv.key == "ARROW:schema").collect();
    tparquet::parquet::arrow::parquet_to_arrow_schema(file.schema_descr(), Some(&arrow)).unwrap()
}

#[test]
fn standard_to_pcodec_and_back() {
    let input = standard();
    let pcodec = run(input.clone(), &Target::Pcodec(WriterOptions::default().with_policy(Policy::Pcodec)));
    assert_eq!(codecs(&pcodec).iter().map(|c| c.1.as_str()).collect::<Vec<_>>(), ["PCODEC", "PCODEC", "ZSTD"]);
    assert_eq!(row_groups(&pcodec), [1000, 1000, 500]);
    assert_eq!(
        kv(&pcodec),
        [
            ("custom".to_string(), Some("kept".to_string())),
            ("parquet.pcodec.version".to_string(), Some("1".to_string()))
        ]
    );
    assert_eq!(arrow_schema(&pcodec), arrow_schema(&input));
    assert_eq!(arrow_schema(&pcodec).metadata()["origin"], "test");
    assert_eq!(lines(&pcodec), lines(&input));
    assert_eq!(verify(input.clone(), pcodec.clone()).unwrap(), None);

    let back = run(pcodec, &Target::standard());
    assert_eq!(codecs(&back).iter().map(|c| c.1.as_str()).collect::<Vec<_>>(), ["ZSTD", "ZSTD", "ZSTD"]);
    assert_eq!(row_groups(&back), [1000, 1000, 500]);
    assert_eq!(kv(&back), [("custom".to_string(), Some("kept".to_string()))]);
    assert_eq!(arrow_schema(&back), arrow_schema(&input));
    assert_eq!(report(&back).pcodec_version, None);
    assert_eq!(verify(input, back).unwrap(), None);
}

#[test]
fn standard_keeps_the_codecs_of_other_columns() {
    let input = standard();
    let options = WriterOptions::default().with_policy(Policy::Base).with_column("id", Policy::Pcodec);
    let pcodec = run(input.clone(), &Target::Pcodec(options.with_base(Compression::SNAPPY)));
    assert_eq!(codecs(&pcodec).iter().map(|c| c.1.as_str()).collect::<Vec<_>>(), ["PCODEC", "SNAPPY", "SNAPPY"]);
    let gzip = Compression::GZIP(GzipLevel::default());
    let back = run(pcodec, &Target::Standard { base: gzip });
    assert_eq!(codecs(&back).iter().map(|c| c.1.as_str()).collect::<Vec<_>>(), ["GZIP", "SNAPPY", "SNAPPY"]);
    assert_eq!(verify(input, back).unwrap(), None);
}

#[test]
fn row_groups_survive_small_batches() {
    // the reader's batches (1024 rows by default) straddle the 1000-row row groups
    let input = standard();
    let out = run(input.clone(), &Target::Pcodec(WriterOptions::default()));
    assert_eq!(row_groups(&out), [1000, 1000, 500]);
    assert_eq!(lines(&out), lines(&input));
}

#[test]
fn files_without_rows_or_arrow_schema() {
    // no row groups at all
    let mut out = Vec::new();
    ArrowWriter::try_new(&mut out, batch(0, 1).schema(), None).unwrap().close().unwrap();
    let empty = run(Bytes::from(out), &Target::Pcodec(WriterOptions::default()));
    assert_eq!(row_groups(&empty), Vec::<i64>::new());
    assert_eq!(kv(&empty), []);
    assert_eq!(arrow_schema(&empty).fields().len(), 3);

    // no ARROW:schema: the schema comes from the Parquet schema
    let props = WriterProperties::builder().build();
    let mut out = Vec::new();
    let opts = tparquet::parquet::arrow::arrow_writer::ArrowWriterOptions::new()
        .with_properties(props)
        .with_skip_arrow_metadata(true);
    let mut w = ArrowWriter::try_new_with_options(&mut out, batch(0, 1).schema(), opts).unwrap();
    w.write(&batch(0, 10)).unwrap();
    w.close().unwrap();
    let input = Bytes::from(out);
    let pcodec = run(input.clone(), &Target::Pcodec(WriterOptions::default()));
    assert!(arrow_schema(&pcodec).metadata().is_empty());
    assert_eq!(lines(&pcodec), lines(&input));
}

#[test]
fn bad_input_is_an_error() {
    let mut out = Vec::new();
    let err = transcode(Bytes::from_static(b"not parquet"), &mut out, &Target::standard()).unwrap_err();
    assert!(err.to_string().starts_with("parquet: "), "{err}");
}

#[test]
fn row_groups_without_rows_are_dropped() {
    // testdata's `empty` file: one row group without rows
    let input = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/files/empty.parquet")).unwrap();
    let out = run(Bytes::from(input), &Target::standard());
    assert_eq!(row_groups(&out), Vec::<i64>::new());
    // a zero-row group between others
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, batch(0, 1).schema(), None).unwrap();
    w.write(&batch(0, 10)).unwrap();
    w.flush().unwrap();
    w.close().unwrap();
    let out = run(Bytes::from(buf), &Target::Pcodec(WriterOptions::default()));
    assert_eq!(row_groups(&out), [10]);
}

/// The Parquet schema of a file, as text.
fn parquet_schema(file: &Bytes) -> String {
    let b = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
    let mut s = Vec::new();
    tparquet::parquet::schema::printer::print_schema(&mut s, b.parquet_schema().root_schema());
    String::from_utf8(s).unwrap()
}

fn sorting(file: &Bytes) -> Vec<Option<Vec<SortingColumn>>> {
    let b = ParquetRecordBatchReaderBuilder::try_new(file.clone()).unwrap();
    b.metadata().row_groups().iter().map(|rg| rg.sorting_columns().cloned()).collect()
}

/// A standard file whose row groups say they are sorted by `id`, then `px` descending.
fn sorted() -> Bytes {
    let sort = vec![
        SortingColumn { column_idx: 0, descending: false, nulls_first: false },
        SortingColumn { column_idx: 1, descending: true, nulls_first: true },
    ];
    let props = WriterProperties::builder().set_sorting_columns(Some(sort)).build();
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, batch(0, 1).schema(), Some(props)).unwrap();
    for (offset, rows) in [(0, 700), (700, 300)] {
        w.write(&batch(offset, rows)).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
    Bytes::from(out)
}

#[test]
fn sorting_columns_are_kept() {
    let input = sorted();
    assert!(sorting(&input).iter().all(|s| s.as_ref().is_some_and(|s| s.len() == 2)));
    let pcodec = run(input.clone(), &Target::Pcodec(WriterOptions::default()));
    assert_eq!(sorting(&pcodec), sorting(&input));
    let back = run(pcodec, &Target::standard());
    assert_eq!(sorting(&back), sorting(&input));
    // row groups that disagree say nothing about the output's
    let mixed = with_row_group_sorting(&input, 1, None);
    let out = run(mixed.clone(), &Target::Pcodec(WriterOptions::default()));
    assert_eq!(sorting(&out), [None, None]);
    assert_eq!(lines(&out), lines(&input));
    let first_only = with_row_group_sorting(&input, 0, Some(vec![]));
    assert_eq!(sorting(&run(first_only, &Target::standard())), [None, None]);
    let unsorted = run(standard(), &Target::Pcodec(WriterOptions::default()));
    assert!(sorting(&unsorted).iter().all(|s| s.is_none()));
}

/// `file` with the sorting columns of row group `rg` replaced by `sort` in the footer.
fn with_row_group_sorting(file: &Bytes, rg: usize, sort: Option<Vec<SortingColumn>>) -> Bytes {
    use tparquet::parquet::file::metadata::{ParquetMetaDataReader, ParquetMetaDataWriter};
    let metadata = ParquetMetaDataReader::new().parse_and_finish(file).unwrap();
    let footer = 8 + u32::from_le_bytes(file[file.len() - 8..file.len() - 4].try_into().unwrap()) as usize;
    let mut groups = metadata.row_groups().to_vec();
    groups[rg] = groups[rg].clone().into_builder().set_sorting_columns(sort).build().unwrap();
    let metadata = metadata.into_builder().set_row_groups(groups).build();
    let mut out = file[..file.len() - footer].to_vec();
    ParquetMetaDataWriter::new(&mut out, &metadata).finish().unwrap();
    Bytes::from(out)
}
