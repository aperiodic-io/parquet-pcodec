//! The DataFusion half of the cross-language acceptance tests (`acceptance/cross/run.sh`):
//!
//! ```text
//! tparquet-datafusion-vectors check DIR          read every DIR/files/*.parquet, compare with DIR/expected
//! tparquet-datafusion-vectors write OUT SRC...   rewrite every SRC vector with COPY ... STORED AS TPARQUET
//! ```
//!
//! `check` prints `<n> vectors match`, or names the first differing line and exits 1.

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(tparquet_datafusion::vectors::main(&args).await);
}
