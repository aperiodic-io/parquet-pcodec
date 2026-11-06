//! Writing: `COPY ... TO ... STORED AS TPARQUET`, `INSERT INTO` a TPARQUET table and
//! `DataFrameExt::write_tparquet`, checked with the fork's metadata API and the page headers
//! (`common::inspect`), and read back bit for bit.

use crate::common::inspect::{FileReport, inspect_file};
use crate::common::*;
use datafusion::arrow::array::builder::{Float64Builder, ListBuilder};
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tparquet_datafusion::{DataFrameExt, Policy, TparquetOptions};

const N: usize = 4000;

/// splitmix64, for reproducible data
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// smooth ints (PCODEC wins), decimal prices (PCODEC wins), long runs of a few random values
/// (dictionary + RLE wins), strings (not eligible)
pub fn mixed(rows: usize, seed: u64) -> RecordBatch {
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

/// Registers `batches` (one partition, in order) as the table `t`.
pub fn register(ctx: &SessionContext, batches: &[RecordBatch]) {
    let _ = ctx.deregister_table("t");
    let table = MemTable::try_new(batches[0].schema(), vec![batches.to_vec()]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
}

/// `COPY (SELECT * FROM t) TO <dir>/<name> STORED AS TPARQUET OPTIONS (<options>)` with `t`
/// holding `batches`; returns the path written.
pub async fn copy(ctx: &SessionContext, dir: &Path, batches: &[RecordBatch], options: &str) -> PathBuf {
    register(ctx, batches);
    let path = dir.join(format!("out{}.parquet", std::fs::read_dir(dir).unwrap().count()));
    let options = if options.is_empty() { String::new() } else { format!(" OPTIONS ({options})") };
    sql(ctx, &format!("COPY (SELECT * FROM t) TO {} STORED AS TPARQUET{options}", lit(&path))).await;
    path
}

/// The canonical rows of the file at `path`, read with DataFusion's Parquet reader.
pub async fn read_back(path: &Path) -> String {
    let ctx = SessionContext::new_with_config(datafusion::prelude::SessionConfig::new().with_target_partitions(1));
    let df = ctx.read_parquet(path.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
    canonical_lines(&df.collect().await.unwrap())
}

fn compressed(r: &FileReport, path: &str) -> i64 {
    r.column(path).compressed_bytes
}

#[tokio::test]
async fn copy_writes_pcodec_plain_v2_for_every_numeric_column() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = mixed(N, 1);
    let path = copy(&ctx, dir.path(), std::slice::from_ref(&batch), "").await;
    let r = inspect_file(&path);
    assert_eq!(r.codecs(), pairs(&[("ts", "PCODEC"), ("price", "PCODEC"), ("flag", "PCODEC"), ("name", "ZSTD")]));
    assert_eq!(r.pcodec_version.as_deref(), Some("1"));
    for c in &r.columns {
        // V2 pages everywhere (the file is written as version 2.0), no dictionary on PCODEC
        assert!(c.pages.keys().all(|k| k == "DATA_PAGE_V2" || (c.path == "name" && k == "DICTIONARY_PAGE")), "{c:?}");
    }
    for c in &r.columns[..3] {
        assert!(c.encodings.iter().all(|e| e == "PLAIN" || e == "RLE"), "{c:?}");
    }
    assert_eq!(r.column("ts").modes.keys().collect::<Vec<_>>(), ["PCO"]);
    assert_eq!(r.column("price").modes.keys().collect::<Vec<_>>(), ["DECIMAL"]);
    assert!(r.column("name").modes.is_empty());
    assert!(r.column("name").encodings.iter().any(|e| e.contains("DICTIONARY")), "{:?}", r.column("name"));
    let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().version(), 2);
    // the Arrow schema is stored, as DataFusion's Parquet sink does
    let kv = reader.metadata().file_metadata().key_value_metadata().unwrap();
    assert!(kv.iter().any(|kv| kv.key == "ARROW:schema"));
    assert_eq!(read_back(&path).await, canonical_lines(&[batch]));
}

#[tokio::test]
async fn base_policy_never_uses_pcodec() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = mixed(N, 2);
    let path = copy(&ctx, dir.path(), std::slice::from_ref(&batch), "policy 'base'").await;
    let r = inspect_file(&path);
    assert_eq!(r.codecs(), pairs(&[("ts", "ZSTD"), ("price", "ZSTD"), ("flag", "ZSTD"), ("name", "ZSTD")]));
    assert_eq!(r.pcodec_version, None);
    // DataFusion's default page version (1.0) when nothing is PCODEC
    assert!(r.columns.iter().all(|c| !c.pages.contains_key("DATA_PAGE_V2")));
    assert_eq!(read_back(&path).await, canonical_lines(&[batch]));
}

#[tokio::test]
async fn base_codec_is_datafusions_compression_option() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = mixed(N, 3);
    let path = copy(&ctx, dir.path(), std::slice::from_ref(&batch), "compression 'snappy'").await;
    assert_eq!(inspect_file(&path).codecs()[3], ("name".to_string(), "SNAPPY".to_string()));
    // the session's setting, and a per-column one
    sql(&ctx, "SET datafusion.execution.parquet.compression = 'gzip(6)'").await;
    let path =
        copy(&ctx, dir.path(), std::slice::from_ref(&batch), "'compression::flag' 'lz4_raw', policy 'base'").await;
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("ts", "GZIP"), ("price", "GZIP"), ("flag", "LZ4_RAW"), ("name", "GZIP")])
    );
    assert_eq!(read_back(&path).await, canonical_lines(&[batch]));
}

#[tokio::test]
async fn auto_keeps_the_smaller_codec_per_column() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batches = [mixed(N, 4), mixed(N, 5)];
    let path = copy(&ctx, dir.path(), &batches, "policy 'auto'").await;
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("ts", "PCODEC"), ("price", "PCODEC"), ("flag", "ZSTD"), ("name", "ZSTD")])
    );
    assert_eq!(read_back(&path).await, canonical_lines(&batches));

    // each chosen codec is really the smaller one on this data
    let one = [mixed(N, 4)];
    let forced = inspect_file(copy(&ctx, dir.path(), &one, "policy 'pcodec'").await);
    let base = inspect_file(copy(&ctx, dir.path(), &one, "policy 'base'").await);
    let auto = inspect_file(copy(&ctx, dir.path(), &one, "policy 'AUTO'").await);
    assert!(compressed(&forced, "ts") < compressed(&base, "ts"));
    assert!(compressed(&forced, "price") < compressed(&base, "price"));
    assert!(compressed(&forced, "flag") > compressed(&base, "flag"));
    assert_eq!(compressed(&auto, "ts"), compressed(&forced, "ts"));
}

#[tokio::test]
async fn auto_decides_on_the_first_batch_with_rows() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = mixed(N, 8);
    let path = copy(&ctx, dir.path(), &[batch.slice(0, 0), batch.clone()], "policy 'auto'").await;
    assert_eq!(inspect_file(&path).codecs()[0], ("ts".to_string(), "PCODEC".to_string()));
    assert_eq!(read_back(&path).await, canonical_lines(&[batch]));
}

#[tokio::test]
async fn column_policies_override_the_default() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = [mixed(N, 6)];
    let path = copy(&ctx, dir.path(), &batch, "policy 'base', 'policy::ts' 'pcodec', 'policy::flag' 'auto'").await;
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("ts", "PCODEC"), ("price", "ZSTD"), ("flag", "ZSTD"), ("name", "ZSTD")])
    );
    let path = copy(&ctx, dir.path(), &batch, "'policy::price' 'base', 'policy::flag' 'pcodec'").await;
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("ts", "PCODEC"), ("price", "ZSTD"), ("flag", "PCODEC"), ("name", "ZSTD")])
    );
    // a policy for a column that is not eligible changes nothing
    let path = copy(&ctx, dir.path(), &batch, "'policy::name' 'pcodec'").await;
    assert_eq!(inspect_file(&path).codecs()[3], ("name".to_string(), "ZSTD".to_string()));
    assert_eq!(read_back(&path).await, canonical_lines(&batch));
}

#[tokio::test]
async fn bad_options_are_errors_before_anything_is_written() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    register(&ctx, &[mixed(10, 7)]);
    let path = dir.path().join("x.parquet");
    let copy =
        |options: &str| format!("COPY (SELECT * FROM t) TO {} STORED AS TPARQUET OPTIONS ({options})", lit(&path));
    for (options, error) in [
        ("'policy::nope' 'pcodec'", "tparquet: policy for \"nope\", but there is no such column"),
        ("policy 'fast'", "tparquet: unknown policy \"fast\": expected auto, pcodec or base"),
        ("pcodec_level '13'", "valid compression range 0..=12 exceeded"),
        ("pcodec_level 'x'", "tparquet: pcodec_level \"x\" is not a level 0 to 12"),
        ("pcodec_level '-1'", "tparquet: pcodec_level \"-1\" is not a level 0 to 12"),
        // DataFusion's own options are still DataFusion's: it refuses pcodec
        ("compression 'pcodec'", "Unknown or unsupported parquet compression: pcodec"),
        ("frobnicate 'yes'", "frobnicate"),
    ] {
        let err = sql_err(&ctx, &copy(options)).await;
        assert!(err.contains(error), "{options}: {err}");
    }
    assert!(!path.exists());
}

#[tokio::test]
async fn level_and_row_group_size_are_used() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = [mixed(N, 11)];
    let level0 = copy(&ctx, dir.path(), &batch, "pcodec_level 0, max_row_group_size 1000").await;
    let r = inspect_file(&level0);
    assert_eq!(r.row_groups, 4);
    assert_eq!(r.column("ts").codecs, ["PCODEC"]);
    let level8 = inspect_file(copy(&ctx, dir.path(), &batch, "max_row_group_size 1000").await);
    let level12 = inspect_file(copy(&ctx, dir.path(), &batch, "pcodec_level 12, max_row_group_size 1000").await);
    // other levels are other encodings of the same data
    assert_ne!(compressed(&r, "price"), compressed(&level8, "price"));
    assert_ne!(compressed(&level12, "price"), compressed(&level8, "price"));
    assert_eq!(read_back(&level0).await, canonical_lines(&batch));
}

#[tokio::test]
async fn nested_leaves_are_chosen_one_by_one() {
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
    let batch = [RecordBatch::try_new(schema, vec![Arc::new(book)]).unwrap()];

    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let path = copy(&ctx, dir.path(), &batch, "").await;
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("book.id", "PCODEC"), ("book.tag", "ZSTD"), ("book.levels.list.item", "PCODEC")])
    );
    assert_eq!(read_back(&path).await, canonical_lines(&batch));
    // an override on a struct covers its leaves; the longest name wins
    let path = copy(&ctx, dir.path(), &batch, "'policy::book' 'base', 'policy::book.levels' 'pcodec'").await;
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("book.id", "ZSTD"), ("book.tag", "ZSTD"), ("book.levels.list.item", "PCODEC")])
    );
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

#[tokio::test]
async fn auto_counts_the_cost_of_v2_pages() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = [v2_penalty(4000)];
    let auto = copy(&ctx, dir.path(), &batch, "policy 'auto'").await;
    let base = copy(&ctx, dir.path(), &batch, "policy 'base'").await;
    let pcodec_t = copy(&ctx, dir.path(), &batch, "policy 'base', 'policy::t' 'pcodec'").await;
    let size = |p: &PathBuf| std::fs::metadata(p).unwrap().len();
    // PCODEC alone is smaller on `t`, but makes the whole file larger (V2 pages)
    assert!(compressed(&inspect_file(&pcodec_t), "t") < compressed(&inspect_file(&base), "t"));
    assert!(size(&pcodec_t) > size(&base));
    assert_eq!(
        inspect_file(&auto).codecs(),
        pairs(&[("t", "ZSTD"), ("bids.list.item", "ZSTD"), ("asks.list.item", "ZSTD")])
    );
    assert_eq!(size(&auto), size(&base));
    assert_eq!(read_back(&auto).await, canonical_lines(&batch));
}

#[tokio::test]
async fn empty_tables_write_files_without_row_groups() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    for policy in ["pcodec", "auto", "base"] {
        let empty = [mixed(0, 9)];
        let path = copy(&ctx, dir.path(), &empty, &format!("policy '{policy}'")).await;
        let r = inspect_file(&path);
        assert_eq!((r.rows, r.row_groups), (0, 0), "{policy}");
        // no PCODEC column chunk, no footer key
        assert_eq!(r.pcodec_version, None, "{policy}");
        assert_eq!(read_back(&path).await, "", "{policy}");
        // the schema is there
        let df = ctx.read_parquet(path.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
        let fields: Vec<String> = df.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(fields, ["ts", "price", "flag", "name"]);
    }
}

#[tokio::test]
async fn every_vector_round_trips_through_copy() {
    // every type the conformance vectors cover: nulls, lists, structs, temporal types,
    // decimals, NaN payloads and signed zeros, several row groups, an empty file
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    for name in vector_names() {
        sql(&ctx, &format!("CREATE EXTERNAL TABLE {name} STORED AS TPARQUET LOCATION {}", lit(&vector_file(&name))))
            .await;
        let out = dir.path().join(format!("{name}.parquet"));
        sql(&ctx, &format!("COPY {name} TO {} STORED AS TPARQUET OPTIONS (max_row_group_size 700)", lit(&out))).await;
        assert!(read_back(&out).await == expected_rows(&name), "{name}");
        let r = inspect_file(&out);
        let original = inspect_file(vector_file(&name));
        assert_eq!(r.rows, original.rows, "{name}");
        assert_eq!(r.row_groups as i64, (r.rows + 699) / 700, "{name}");
        for c in &r.columns {
            let eligible = ["INT32", "INT64", "FLOAT", "DOUBLE"].contains(&c.physical_type.as_str());
            if r.rows > 0 {
                assert_eq!(c.codecs, [if eligible { "PCODEC" } else { "ZSTD" }], "{name} {c:?}");
            }
            if eligible {
                assert!(c.pages.keys().all(|k| k == "DATA_PAGE_V2"), "{name} {c:?}");
                assert!(
                    c.modes.keys().all(|m| ["PCO", "DECIMAL", "RAW", "EMPTY", "UNCOMPRESSED"].contains(&m.as_str())),
                    "{name} {c:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn copy_partitioned_by_writes_a_hive_directory() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    sql(
        &ctx,
        &format!("CREATE EXTERNAL TABLE trades STORED AS PARQUET LOCATION {}", lit(&vector_file("standard_control"))),
    )
    .await;
    let out = dir.path().join("by_symbol");
    let n = scalar(
        &ctx,
        &format!("COPY (SELECT * FROM trades) TO {} STORED AS TPARQUET PARTITIONED BY (symbol)", lit(&out)),
    )
    .await;
    assert_eq!(n, "1500");
    let mut parts: Vec<String> =
        std::fs::read_dir(&out).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
    parts.sort();
    assert_eq!(parts, ["symbol=ADA-USD", "symbol=BTC-USD", "symbol=ETH-USD", "symbol=SOL-USD"]);
    for p in &parts {
        for f in std::fs::read_dir(out.join(p)).unwrap() {
            let r = inspect_file(f.unwrap().path());
            // the partition column is not in the files
            assert!(r.columns.iter().all(|c| c.path != "symbol"));
            assert_eq!(r.column("price").codecs, ["PCODEC"]);
            assert_eq!(r.column("venue").codecs, ["ZSTD"]);
        }
    }
    // read back as a partitioned table
    sql(&ctx, &format!("CREATE EXTERNAL TABLE parts STORED AS TPARQUET LOCATION '{}/'", out.display())).await;
    let q = "SELECT symbol, count(*) AS n, sum(qty) AS q, min(ts) AS lo, max(price) AS hi FROM {} GROUP BY symbol ORDER BY symbol";
    let want = canonical_sql(&ctx, &q.replace("{}", "trades")).await;
    assert_eq!(canonical_sql(&ctx, &q.replace("{}", "parts")).await, want);
}

#[tokio::test]
async fn insert_into_a_tparquet_table() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    sql(
        &ctx,
        &format!(
            "CREATE EXTERNAL TABLE ticks (ts BIGINT NOT NULL, price DOUBLE, venue VARCHAR) STORED AS TPARQUET \
             LOCATION '{}/' OPTIONS (policy 'pcodec')",
            dir.path().display()
        ),
    )
    .await;
    assert_eq!(scalar(&ctx, "INSERT INTO ticks VALUES (1, 100.25, 'a'), (2, -0.0, NULL), (3, 101.5, 'b')").await, "3");
    assert_eq!(scalar(&ctx, "INSERT INTO ticks SELECT ts + 10, price * 2, venue FROM ticks").await, "3");
    let files: Vec<PathBuf> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().path()).collect();
    assert_eq!(files.len(), 2);
    for f in &files {
        assert_eq!(f.extension().unwrap(), "parquet");
        let r = inspect_file(f);
        assert_eq!(r.codecs(), pairs(&[("ts", "PCODEC"), ("price", "PCODEC"), ("venue", "ZSTD")]));
    }
    assert_eq!(scalar(&ctx, "SELECT sum(ts) FROM ticks").await, "42");
    assert_eq!(
        canonical_sql(&ctx, "SELECT ts, price FROM ticks WHERE ts < 10 ORDER BY ts").await,
        "{\"ts\":1,\"price\":\"0x4059100000000000\"}\n{\"ts\":2,\"price\":\"0x8000000000000000\"}\n\
         {\"ts\":3,\"price\":\"0x4059600000000000\"}\n"
    );
    // INSERT OVERWRITE is not supported, as for DataFusion's Parquet format
    let err = sql_err(&ctx, "INSERT OVERWRITE ticks VALUES (4, 1.0, 'c')").await;
    assert!(err.contains("Overwrite"), "{err}");
}

#[tokio::test]
async fn dataframe_write_tparquet() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = mixed(N, 13);
    register(&ctx, std::slice::from_ref(&batch));
    let path = dir.path().join("df.parquet");
    let df = ctx.table("t").await.unwrap();
    let options = TparquetOptions::default().with_policy(Policy::Base).with_column("price", Policy::Pcodec);
    let n = df.write_tparquet(path.to_str().unwrap(), options, None).await.unwrap();
    let count = datafusion::arrow::util::pretty::pretty_format_batches(&n).unwrap().to_string();
    assert!(count.contains("4000"), "{count}");
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("ts", "ZSTD"), ("price", "PCODEC"), ("flag", "ZSTD"), ("name", "ZSTD")])
    );
    assert_eq!(read_back(&path).await, canonical_lines(std::slice::from_ref(&batch)));

    // with Parquet options of its own
    let path = dir.path().join("df_snappy.parquet");
    let mut parquet = datafusion::common::config::TableParquetOptions::default();
    parquet.global.compression = Some("snappy".into());
    let df = ctx.table("t").await.unwrap();
    df.write_tparquet(path.to_str().unwrap(), TparquetOptions::default(), Some(parquet)).await.unwrap();
    assert_eq!(
        inspect_file(&path).codecs(),
        pairs(&[("ts", "PCODEC"), ("price", "PCODEC"), ("flag", "PCODEC"), ("name", "SNAPPY")])
    );
}

#[tokio::test]
async fn sorted_copies_record_their_sorting_columns() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    register(&ctx, &[mixed(N, 14)]);
    let sorted = dir.path().join("sorted.parquet");
    sql(
        &ctx,
        &format!("COPY (SELECT * FROM t ORDER BY price DESC NULLS LAST, ts) TO {} STORED AS TPARQUET", lit(&sorted)),
    )
    .await;
    let reader = SerializedFileReader::new(std::fs::File::open(&sorted).unwrap()).unwrap();
    let columns = reader.metadata().row_group(0).sorting_columns().unwrap().clone();
    let got: Vec<(i32, bool, bool)> = columns.iter().map(|c| (c.column_idx, c.descending, c.nulls_first)).collect();
    assert_eq!(got, [(1, true, false), (0, false, false)]);
    // DataFusion reads the ordering back from the file
    sql(&ctx, &format!("CREATE EXTERNAL TABLE s STORED AS TPARQUET LOCATION {}", lit(&sorted))).await;
    let plan = canonical_sql(&ctx, "EXPLAIN SELECT * FROM s").await;
    assert!(plan.contains("output_ordering=[price@1 DESC NULLS LAST, ts@0 ASC NULLS LAST]"), "{plan}");

    // an ordering on an expression is left out of the metadata
    let expr = dir.path().join("expr.parquet");
    sql(&ctx, &format!("COPY (SELECT * FROM t ORDER BY price + 1) TO {} STORED AS TPARQUET", lit(&expr))).await;
    let reader = SerializedFileReader::new(std::fs::File::open(&expr).unwrap()).unwrap();
    assert_eq!(reader.metadata().row_group(0).sorting_columns(), None);
    // and so are partition columns, which the files do not hold
    let parts = dir.path().join("parts");
    sql(
        &ctx,
        &format!(
            "COPY (SELECT * FROM t ORDER BY name, ts) TO {} STORED AS TPARQUET PARTITIONED BY (name)",
            lit(&parts)
        ),
    )
    .await;
    let part = std::fs::read_dir(parts.join("name=n0")).unwrap().next().unwrap().unwrap().path();
    let reader = SerializedFileReader::new(std::fs::File::open(&part).unwrap()).unwrap();
    let columns = reader.metadata().row_group(0).sorting_columns().unwrap();
    assert_eq!(columns.iter().map(|c| c.column_idx).collect::<Vec<_>>(), [0]);
}

#[tokio::test]
async fn explain_analyze_shows_the_sink_and_its_metrics() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    register(&ctx, &[mixed(N, 15)]);
    let path = dir.path().join("x.parquet");
    let plan =
        canonical_sql(&ctx, &format!("EXPLAIN ANALYZE COPY (SELECT * FROM t) TO {} STORED AS TPARQUET", lit(&path)))
            .await;
    assert!(plan.contains("TparquetSink(file_groups=[])"), "{plan}");
    assert!(plan.contains("rows_written=4.00 K") || plan.contains("rows_written=4000"), "{plan}");
    assert!(plan.contains("bytes_written="), "{plan}");
}

#[tokio::test]
async fn skip_arrow_metadata_and_other_parquet_options_are_kept() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    let batch = [mixed(N, 16)];
    let path = copy(
        &ctx,
        dir.path(),
        &batch,
        "skip_arrow_metadata 'true', 'metadata::origin' 'tests', bloom_filter_on_write 'true', statistics_enabled 'none'",
    )
    .await;
    let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
    let kv = reader.metadata().file_metadata().key_value_metadata().unwrap();
    let mut keys: Vec<&str> = kv.iter().map(|kv| kv.key.as_str()).collect();
    keys.sort();
    assert_eq!(keys, ["origin", "parquet.pcodec.version"]);
    let rg = reader.metadata().row_group(0);
    assert!(rg.column(0).bloom_filter_offset().is_some());
    assert!(rg.column(0).statistics().is_none());
    assert_eq!(read_back(&path).await, canonical_lines(&batch));
}
