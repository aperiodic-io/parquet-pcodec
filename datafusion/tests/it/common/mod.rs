//! Helpers shared by the integration tests.
#![allow(dead_code)] // each test binary uses a different subset

pub mod inspect;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tparquet_datafusion::canonical;

/// The repository's `testdata/`.
pub fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../testdata")
}

/// `testdata/<sub>/manifest.json`.
pub fn manifest(sub: &str) -> Value {
    serde_json::from_slice(&std::fs::read(testdata().join(sub).join("manifest.json")).unwrap()).unwrap()
}

/// Names of every conformance vector file, in manifest order.
pub fn vector_names() -> Vec<String> {
    manifest("files")["files"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap().to_string()).collect()
}

/// `testdata/files/<name>.parquet`.
pub fn vector_file(name: &str) -> PathBuf {
    testdata().join("files").join(format!("{name}.parquet"))
}

/// `testdata/expected/<name>.jsonl`.
pub fn expected_rows(name: &str) -> String {
    std::fs::read_to_string(testdata().join("expected").join(format!("{name}.jsonl"))).unwrap()
}

/// A session with the TPARQUET format registered. One target partition, so that a scan
/// returns the rows of a file in file order.
pub fn ctx() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    tparquet_datafusion::register_tparquet(&ctx).unwrap();
    ctx
}

/// A session with the TPARQUET format registered and several partitions.
pub fn parallel_ctx() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    tparquet_datafusion::register_tparquet(&ctx).unwrap();
    ctx
}

/// The canonical JSON lines of `batches`.
pub fn canonical_lines(batches: &[RecordBatch]) -> String {
    let mut out = String::new();
    for b in batches {
        canonical::write_batch(b, &mut out).unwrap();
    }
    out
}

/// Runs `sql` and collects its batches.
pub async fn sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")).collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Runs `sql` and returns the error, planning or execution.
pub async fn sql_err(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(b) => panic!("{sql}: expected an error, got {} batches", b.len()),
        },
    }
}

/// Runs `sql` and returns its canonical rows.
pub async fn canonical_sql(ctx: &SessionContext, query: &str) -> String {
    canonical_lines(&sql(ctx, query).await)
}

/// The single value of a one-row, one-column result, as text.
pub async fn scalar(ctx: &SessionContext, query: &str) -> String {
    let batches = sql(ctx, query).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "{query}");
    let b = batches.iter().find(|b| b.num_rows() == 1).unwrap();
    datafusion::arrow::util::display::array_value_to_string(b.column(0), 0).unwrap()
}

/// A SQL string literal of `path`.
pub fn lit(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

/// Quotes `name` as a SQL identifier.
pub fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
