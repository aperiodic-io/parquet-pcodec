mod common;

use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use common::{Rng, batch_lines, codecs, lines, report, write};
use std::sync::Arc;
use tparquet::parquet::basic::{Compression, GzipLevel};
use tparquet::parquet::file::properties::WriterProperties;
use tparquet::{Policy, Writer, WriterOptions};

const N: usize = 4000;

/// smooth ints (PCODEC wins), decimal prices (PCODEC wins), long runs of a few random values
/// (dictionary + RLE wins), strings (not eligible)
fn mixed(rows: usize, seed: u64) -> RecordBatch {
    let mut rng = Rng(seed);
    let ts = TimestampMillisecondArray::from_iter_values((0..rows as i64).map(|i| 1_700_000_000_000 + i * 97));
    let mut p = 100_000i64;
    let price = Float64Array::from_iter_values((0..rows).map(|_| {
        p += rng.below(5) as i64 - 2;
        p as f64 / 100.0
    }));
    let runs: Vec<i64> = (0..8).map(|_| rng.next() as i64).collect();
    let flag = Int64Array::from_iter_values((0..rows).map(|i| runs[(i / 512) % 8]));
    let name = StringArray::from_iter_values((0..rows).map(|i| format!("n{}", i % 13)));
    RecordBatch::try_from_iter([
        ("ts", Arc::new(ts) as ArrayRef),
        ("price", Arc::new(price) as ArrayRef),
        ("flag", Arc::new(flag) as ArrayRef),
        ("name", Arc::new(name) as ArrayRef),
    ])
    .unwrap()
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect()
}

#[test]
fn pcodec_policy_uses_pcodec_plain_v2_for_every_numeric_column() {
    let batch = mixed(N, 1);
    let (file, chosen) = write(std::slice::from_ref(&batch), WriterOptions::default().with_policy(Policy::Pcodec));
    assert_eq!(codecs(&file), pairs(&[("ts", "PCODEC"), ("price", "PCODEC"), ("flag", "PCODEC"), ("name", "ZSTD")]));
    let chosen = chosen.unwrap();
    assert_eq!(chosen[0].path, "ts");
    assert_eq!(chosen[0].codec, Compression::PCODEC(Default::default()));
    assert_eq!(chosen[3].codec, Compression::ZSTD(tparquet::parquet::basic::ZstdLevel::try_new(3).unwrap()));
    let r = report(&file);
    assert_eq!(r.pcodec_version.as_deref(), Some("1"));
    for c in &r.columns {
        // V2 pages everywhere (the file is written as version 2.0), no dictionary on PCODEC
        assert!(c.pages.keys().all(|k| k == "DATA_PAGE_V2" || (c.path == "name" && k == "DICTIONARY_PAGE")), "{c:?}");
    }
    assert_eq!(r.columns[0].modes.keys().collect::<Vec<_>>(), ["PCO"]);
    assert_eq!(r.columns[1].modes.keys().collect::<Vec<_>>(), ["DECIMAL"]);
    assert!(r.columns[3].modes.is_empty());
    let metadata = tparquet::parquet::file::reader::SerializedFileReader::new(file.clone()).unwrap();
    use tparquet::parquet::file::reader::FileReader;
    let rg = metadata.metadata().row_group(0);
    assert_eq!(metadata.metadata().file_metadata().version(), 2);
    for i in 0..3 {
        let encodings: Vec<String> = rg.column(i).encodings().map(|e| e.to_string()).collect();
        assert!(encodings.iter().all(|e| e == "PLAIN" || e == "RLE"), "{encodings:?}");
    }
    assert_eq!(lines(&file), batch_lines(&[batch]));
}

#[test]
fn base_policy_never_uses_pcodec() {
    let batch = mixed(N, 2);
    let (file, _) = write(std::slice::from_ref(&batch), WriterOptions::default().with_policy(Policy::Base));
    assert_eq!(codecs(&file), pairs(&[("ts", "ZSTD"), ("price", "ZSTD"), ("flag", "ZSTD"), ("name", "ZSTD")]));
    let r = report(&file);
    assert_eq!(r.pcodec_version, None);
    assert!(r.columns.iter().all(|c| !c.pages.contains_key("DATA_PAGE_V2")));
    assert_eq!(lines(&file), batch_lines(&[batch]));
}

#[test]
fn base_codec_is_configurable() {
    let batch = mixed(N, 3);
    let gzip = Compression::GZIP(GzipLevel::default());
    let (file, _) = write(&[batch], WriterOptions::default().with_policy(Policy::Pcodec).with_base(gzip));
    assert_eq!(codecs(&file)[3], ("name".to_string(), "GZIP".to_string()));
}

#[test]
fn auto_keeps_the_smaller_codec_per_column() {
    let batch = mixed(N, 4);
    let (file, chosen) = write(&[batch.clone(), mixed(N, 5)], WriterOptions::default());
    assert_eq!(codecs(&file), pairs(&[("ts", "PCODEC"), ("price", "PCODEC"), ("flag", "ZSTD"), ("name", "ZSTD")]));
    assert_eq!(chosen.unwrap()[2].codec, tparquet::writer::default_base());
    assert_eq!(lines(&file), batch_lines(&[batch, mixed(N, 5)]));

    // each chosen codec is really the smaller one on this data
    let (forced, _) = write(&[mixed(N, 4)], WriterOptions::default().with_policy(Policy::Pcodec));
    let (base, _) = write(&[mixed(N, 4)], WriterOptions::default().with_policy(Policy::Base));
    let (auto, _) = write(&[mixed(N, 4)], WriterOptions::default());
    let size = |f: &bytes::Bytes, i: usize| report(f).columns[i].compressed_bytes;
    assert!(size(&forced, 0) < size(&base, 0));
    assert!(size(&forced, 1) < size(&base, 1));
    assert!(size(&forced, 2) > size(&base, 2));
    assert_eq!(size(&auto, 0), size(&forced, 0));
}

#[test]
fn column_policies_override_the_default() {
    let batch = mixed(N, 6);
    let options = WriterOptions::default()
        .with_policy(Policy::Base)
        .with_column("ts", Policy::Pcodec)
        .with_column("flag", Policy::Auto);
    let (file, _) = write(&[batch], options);
    assert_eq!(codecs(&file), pairs(&[("ts", "PCODEC"), ("price", "ZSTD"), ("flag", "ZSTD"), ("name", "ZSTD")]));

    let options = WriterOptions::default().with_column("price", Policy::Base).with_column("flag", Policy::Pcodec);
    let (file, _) = write(&[mixed(N, 6)], options);
    assert_eq!(codecs(&file), pairs(&[("ts", "PCODEC"), ("price", "ZSTD"), ("flag", "PCODEC"), ("name", "ZSTD")]));
}

#[test]
fn unknown_columns_and_levels_are_errors() {
    let batch = mixed(10, 7);
    let err = Writer::try_new(Vec::new(), batch.schema(), WriterOptions::default().with_column("nope", Policy::Pcodec))
        .err()
        .unwrap();
    assert_eq!(err.to_string(), "no column named \"nope\"");
    assert!(
        Writer::try_new(Vec::new(), batch.schema(), WriterOptions::default().with_column("ts", Policy::Base)).is_ok()
    );
    assert_eq!(
        WriterOptions::default().with_level(13).unwrap_err().to_string(),
        "parquet: Parquet error: valid compression range 0..=12 exceeded."
    );
    assert_eq!(WriterOptions::default().with_level(2).unwrap().level.compression_level(), 2);
}

#[test]
fn auto_decides_on_the_first_batch_with_rows() {
    let batch = mixed(N, 8);
    let mut out = Vec::new();
    let mut w = Writer::try_new(&mut out, batch.schema(), WriterOptions::default()).unwrap();
    assert!(w.codecs().is_none());
    w.write(&batch.slice(0, 0)).unwrap();
    w.flush().unwrap();
    assert!(w.codecs().is_none());
    w.write(&batch).unwrap();
    assert_eq!(w.codecs().unwrap()[0].codec, Compression::PCODEC(Default::default()));
    w.close().unwrap();
    assert_eq!(lines(&bytes::Bytes::from(out)), batch_lines(&[batch]));
}

#[test]
fn auto_without_rows_uses_the_base_codec() {
    let batch = mixed(0, 9);
    let mut out = Vec::new();
    let w = Writer::try_new(&mut out, batch.schema(), WriterOptions::default()).unwrap();
    let metadata = w.close().unwrap();
    assert_eq!(metadata.num_row_groups(), 0);
    assert_eq!(metadata.file_metadata().num_rows(), 0);
    let file = bytes::Bytes::from(out);
    assert!(lines(&file).is_empty());
    assert_eq!(report(&file).pcodec_version, None);
}

#[test]
fn finish_returns_the_codecs_written() {
    // without rows, Auto columns get the base codec and forced ones keep theirs
    let empty = mixed(0, 9);
    let options = WriterOptions::default().with_column("price", Policy::Pcodec);
    let w = Writer::try_new(Vec::new(), empty.schema(), options).unwrap();
    assert!(w.codecs().is_none());
    let (metadata, chosen) = w.finish().unwrap();
    assert_eq!(metadata.num_row_groups(), 0);
    let names: Vec<(String, String)> = chosen.iter().map(|c| (c.path.clone(), c.codec.to_string())).collect();
    let zstd = Compression::ZSTD(tparquet::parquet::basic::ZstdLevel::try_new(3).unwrap()).to_string();
    let pcodec = Compression::PCODEC(Default::default()).to_string();
    assert_eq!(
        names,
        [("ts", &zstd), ("price", &pcodec), ("flag", &zstd), ("name", &zstd)]
            .map(|(p, c)| (p.to_string(), c.clone()))
            .to_vec()
    );
    // with rows: what codecs() said, and the file it wrote
    let batch = mixed(N, 11);
    let mut out = Vec::new();
    let mut w = Writer::try_new(&mut out, batch.schema(), WriterOptions::default()).unwrap();
    w.write(&batch).unwrap();
    let before = w.codecs().unwrap();
    let (metadata, after) = w.finish().unwrap();
    assert_eq!(before, after);
    assert_eq!(metadata.file_metadata().num_rows(), N as i64);
    assert_eq!(lines(&bytes::Bytes::from(out)), batch_lines(&[batch]));
}

#[test]
fn forced_policies_open_at_once() {
    let batch = mixed(N, 10);
    let mut out = Vec::new();
    let mut w =
        Writer::try_new(&mut out, batch.schema(), WriterOptions::default().with_policy(Policy::Pcodec)).unwrap();
    assert_eq!(w.codecs().unwrap().len(), 4);
    w.write(&batch).unwrap();
    w.flush().unwrap();
    w.write(&batch).unwrap();
    let metadata = w.close().unwrap();
    assert_eq!(metadata.num_row_groups(), 2);
}

#[test]
fn level_and_properties_are_used() {
    let batch = mixed(N, 11);
    let props = WriterProperties::builder().set_max_row_group_row_count(Some(1000)).build();
    let options = WriterOptions::default().with_policy(Policy::Pcodec).with_level(0).unwrap().with_properties(props);
    let (file, chosen) = write(std::slice::from_ref(&batch), options);
    assert_eq!(
        chosen.unwrap()[0].codec,
        Compression::PCODEC(tparquet::parquet::basic::PcodecLevel::try_new(0).unwrap())
    );
    assert_eq!(report(&file).row_groups, 4);
    let (level8, _) = write(std::slice::from_ref(&batch), WriterOptions::default().with_policy(Policy::Pcodec));
    // level 0 is a different (here: larger) encoding of the same data
    assert_ne!(report(&file).columns[1].compressed_bytes / 4, report(&level8).columns[1].compressed_bytes);
    assert_eq!(lines(&file), batch_lines(&[batch]));
}

#[test]
fn nested_leaves_are_chosen_one_by_one() {
    let rows = 2000;
    let mut levels = ListBuilder::new(Float64Builder::new());
    for i in 0..rows {
        levels.append_value((0..5).map(|j| Some(100.0 + (i + j) as f64 / 100.0)));
    }
    let levels: ArrayRef = Arc::new(levels.finish());
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..rows));
    let tags: ArrayRef = Arc::new(StringArray::from_iter_values((0..rows).map(|i| format!("t{}", i % 3))));
    let book = StructArray::from(vec![
        (Arc::new(Field::new("id", DataType::Int32, false)), ids),
        (Arc::new(Field::new("tag", DataType::Utf8, false)), tags),
        (Arc::new(Field::new("levels", levels.data_type().clone(), false)), levels),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new("book", book.data_type().clone(), false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(book)]).unwrap();

    let (file, _) = write(std::slice::from_ref(&batch), WriterOptions::default().with_policy(Policy::Pcodec));
    assert_eq!(
        codecs(&file),
        pairs(&[("book.id", "PCODEC"), ("book.tag", "ZSTD"), ("book.levels.list.item", "PCODEC")])
    );
    assert_eq!(lines(&file), batch_lines(std::slice::from_ref(&batch)));

    let options = WriterOptions::default().with_policy(Policy::Pcodec).with_column("book.levels", Policy::Base);
    let (file, _) = write(std::slice::from_ref(&batch), options);
    assert_eq!(codecs(&file)[2].1, "ZSTD");
    assert_eq!(codecs(&file)[0].1, "PCODEC");
}

/// a PCODEC gain on `t`, and order-book-like price lists that V2 pages make larger
fn v2_penalty(rows: usize) -> RecordBatch {
    let mut rng = Rng(12);
    let mut mid = 6_500_000i64;
    let mut bids = ListBuilder::new(Float64Builder::new());
    let mut asks = ListBuilder::new(Float64Builder::new());
    for _ in 0..rows {
        mid += rng.below(5) as i64 - 2;
        bids.append_value((0..20).map(|k| Some((mid - k) as f64 / 100.0)));
        asks.append_value((0..20).map(|k| Some((mid + 1 + k) as f64 / 100.0)));
    }
    RecordBatch::try_from_iter([
        ("t", Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef),
        ("bids", Arc::new(bids.finish()) as ArrayRef),
        ("asks", Arc::new(asks.finish()) as ArrayRef),
    ])
    .unwrap()
}

#[test]
fn auto_counts_the_cost_of_v2_pages() {
    let batch = v2_penalty(4000);
    let (auto, _) = write(std::slice::from_ref(&batch), WriterOptions::default());
    let (base, _) = write(std::slice::from_ref(&batch), WriterOptions::default().with_policy(Policy::Base));
    let (pcodec_t, _) = write(
        std::slice::from_ref(&batch),
        WriterOptions::default().with_policy(Policy::Base).with_column("t", Policy::Pcodec),
    );
    // PCODEC alone is smaller on `t`, but makes the whole file larger (V2 pages)
    assert!(report(&pcodec_t).columns[0].compressed_bytes < report(&base).columns[0].compressed_bytes);
    assert!(pcodec_t.len() > base.len());
    assert_eq!(codecs(&auto), pairs(&[("t", "ZSTD"), ("bids.list.item", "ZSTD"), ("asks.list.item", "ZSTD")]));
    assert_eq!(auto.len(), base.len());
    assert_eq!(lines(&auto), batch_lines(&[batch]));
}
