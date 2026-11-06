//! The Rust API beyond SQL: the format created by the factory, the `FileFormat` methods
//! DataFusion's SQL paths do not all call, and the sink of a `COPY` plan.

use crate::common::*;
use crate::write::{mixed, register};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::HashMap;
use std::sync::Arc;
use tparquet_datafusion::{Policy, TparquetFormat, TparquetFormatFactory, TparquetOptions, TparquetSink};

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn the_factory_splits_its_options_between_parquet_and_tparquet() {
    let state = ctx().state();
    let options = map(&[("format.policy", "auto"), ("format.pcodec_level", "3"), ("format.compression", "snappy")]);
    let format = TparquetFormatFactory::new().create(&state, &options).unwrap();
    let format = format.downcast_ref::<TparquetFormat>().unwrap();
    assert_eq!(format.options(), &TparquetOptions::default().with_policy(Policy::Auto).with_level(3).unwrap());
    assert_eq!(format.parquet().options().global.compression.as_deref(), Some("snappy"));

    // the factory's own options are the defaults the statement's apply to
    let factory = TparquetFormatFactory::new_with_options(TparquetOptions::default().with_column("ts", Policy::Base));
    let format = factory.create(&state, &map(&[("format.policy", "base")])).unwrap();
    let format = format.downcast_ref::<TparquetFormat>().unwrap();
    assert_eq!(format.options().policy, Policy::Base);
    assert_eq!(format.options().columns, [("ts".to_string(), Policy::Base)]);
    // and its Parquet options replace the session's
    let mut parquet = datafusion::common::config::TableParquetOptions::default();
    parquet.global.max_row_group_size = 7;
    let format = TparquetFormatFactory::new().with_parquet_options(parquet).create(&state, &HashMap::new()).unwrap();
    let format = format.downcast_ref::<TparquetFormat>().unwrap();
    assert_eq!(format.parquet().options().global.max_row_group_size, 7);
}

#[tokio::test]
async fn statistics_and_ordering_are_inferred_as_for_parquet() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    register(&ctx, &[mixed(3000, 50)]);
    let sorted = dir.path().join("sorted.parquet");
    sql(&ctx, &format!("COPY (SELECT * FROM t ORDER BY ts) TO {} STORED AS TPARQUET", lit(&sorted))).await;

    let state = ctx.state();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let meta = store.head(&ObjectPath::from_absolute_path(&sorted).unwrap()).await.unwrap();
    let format = TparquetFormat::default();
    let schema = format.infer_schema(&state, &store, std::slice::from_ref(&meta)).await.unwrap();
    assert_eq!(schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(), ["ts", "price", "flag", "name"]);

    let stats = format.infer_stats(&state, &store, schema.clone(), &meta).await.unwrap();
    assert_eq!(stats.num_rows.get_value(), Some(&3000));
    let ordering = format.infer_ordering(&state, &store, schema.clone(), &meta).await.unwrap();
    assert_eq!(ordering.map(|o| o.to_string()), Some("ts@0 ASC NULLS LAST".to_string()));
    let both = format.infer_stats_and_ordering(&state, &store, schema.clone(), &meta).await.unwrap();
    assert_eq!(both.statistics.num_rows.get_value(), Some(&3000));
    assert!(both.ordering.is_some());

    // an unsorted PCODEC file: no ordering
    let meta = store
        .head(&ObjectPath::from_absolute_path(vector_file("mixed").canonicalize().unwrap()).unwrap())
        .await
        .unwrap();
    let schema = format.infer_schema(&state, &store, std::slice::from_ref(&meta)).await.unwrap();
    assert_eq!(format.infer_ordering(&state, &store, schema, &meta).await.unwrap(), None);
}

/// The `TparquetSink` of a physical plan.
fn sink_of(plan: &Arc<dyn ExecutionPlan>) -> &TparquetSink {
    let exec = plan.downcast_ref::<DataSinkExec>().expect("a DataSinkExec");
    exec.sink().downcast_ref::<TparquetSink>().expect("a TparquetSink")
}

#[tokio::test]
async fn the_sink_reports_the_files_it_wrote() {
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    register(&ctx, &[mixed(2500, 51)]);
    let out = dir.path().join("parts");
    let copy = format!(
        "COPY (SELECT *, CAST(flag % 2 AS VARCHAR) AS half FROM t) TO '{}/' STORED AS TPARQUET PARTITIONED BY (half)",
        out.display()
    );
    let plan = ctx.sql(&copy).await.unwrap().create_physical_plan().await.unwrap();
    let sink = sink_of(&plan);
    assert_eq!(format!("{sink:?}"), "TparquetSink");
    assert!(sink.written().is_empty());
    collect(plan.clone(), ctx.task_ctx()).await.unwrap();
    let written = sink.written();
    let rows: i64 = written.values().map(|m| m.file_metadata().num_rows()).sum();
    assert_eq!(rows, 2500);
    assert!(!written.is_empty());
    for (path, metadata) in &written {
        assert!(path.as_ref().contains("half="), "{path}");
        assert!(path.as_ref().ends_with(".parquet"), "{path}");
        assert!(matches!(metadata.row_group(0).column(0).compression(), parquet::basic::Compression::PCODEC(_)));
    }
}

#[tokio::test]
async fn encryption_options_are_refused() {
    use tparquet_datafusion::DataFrameExt;
    let ctx = ctx();
    let dir = tempfile::tempdir().unwrap();
    register(&ctx, &[mixed(10, 52)]);
    let path = dir.path().join("x.parquet");
    let q = format!("COPY t TO {} STORED AS TPARQUET OPTIONS ('format.crypto.factory_id' 'kms')", lit(&path));
    let err = sql_err(&ctx, &q).await;
    assert!(err.contains("TPARQUET does not write encrypted files"), "{err}");
    // either way of asking for encryption
    let mut parquet = datafusion::common::config::TableParquetOptions::default();
    parquet.crypto.file_encryption = Some(Default::default());
    let df = ctx.table("t").await.unwrap();
    let err = df.write_tparquet(path.to_str().unwrap(), TparquetOptions::default(), Some(parquet)).await.unwrap_err();
    assert!(err.to_string().contains("TPARQUET does not write encrypted files"), "{err}");
    assert!(!path.exists());
}
