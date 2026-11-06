//! PCODEC Parquet files (`spec/FORMAT.md`) in Apache DataFusion.
//!
//! The crate is built against `datafusion/parquet`, a fork of the `parquet` crate 59.3.2 with
//! the PCODEC codec (`parquet/FORK.md`), patched in with `[patch.crates-io]`. With the fork,
//! DataFusion's own Parquet reader reads PCODEC files: `STORED AS PARQUET`,
//! `ctx.read_parquet`, `ctx.register_parquet` and `SELECT * FROM 'file.parquet'` need nothing
//! from this crate. What it adds:
//!
//! - [`register_tparquet`]: the `TPARQUET` format, for `CREATE EXTERNAL TABLE ... STORED AS
//!   TPARQUET` (reads like `PARQUET`) and `COPY ... TO ... STORED AS TPARQUET` / `INSERT INTO`
//!   (writes PCODEC for the `INT32`, `INT64`, `FLOAT` and `DOUBLE` columns, the base codec,
//!   DataFusion's `compression` option, for the others).
//! - [`TparquetFormatFactory`], [`TparquetFormat`], [`TparquetSink`]: the pieces, for a
//!   `SessionState` built by hand.
//! - [`SessionContextExt`] and [`DataFrameExt`]: `read_tparquet`, `register_tparquet`,
//!   `write_tparquet`.
//! - [`canonical`]: the canonical JSON rows of `testdata/README.md`, and [`vectors`], the
//!   cross-language acceptance checker built on it.
//!
//! ```no_run
//! # async fn example() -> datafusion::error::Result<()> {
//! use datafusion::prelude::SessionContext;
//!
//! let ctx = SessionContext::new();
//! tparquet_datafusion::register_tparquet(&ctx)?;
//! ctx.sql("COPY (SELECT * FROM 'trades.parquet') TO 'trades.pcodec.parquet' STORED AS TPARQUET")
//!     .await?
//!     .collect()
//!     .await?;
//! ctx.sql("CREATE EXTERNAL TABLE trades STORED AS TPARQUET LOCATION 'trades.pcodec.parquet'").await?;
//! # Ok(())
//! # }
//! ```

pub mod canonical;
mod codecs;
mod ext;
mod format;
mod options;
mod sink;
pub mod vectors;

pub use ext::{DataFrameExt, SessionContextExt};
pub use format::{TPARQUET, TparquetFormat, TparquetFormatFactory};
pub use options::{LEVEL_KEY, POLICY_KEY, Policy, TparquetOptions};
pub use parquet;
pub use sink::TparquetSink;

use datafusion::datasource::listing_table_factory::ListingTableFactory;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Registers the `TPARQUET` format in `state`: its file format (for `COPY ... STORED AS
/// TPARQUET`) and its table factory (for `CREATE EXTERNAL TABLE ... STORED AS TPARQUET`).
pub fn register_tparquet_state(state: &mut SessionState) -> Result<()> {
    state.register_file_format(Arc::new(TparquetFormatFactory::new()), true)?;
    state.table_factories_mut().insert(TPARQUET.to_ascii_uppercase(), Arc::new(ListingTableFactory::new()));
    Ok(())
}

/// Registers the `TPARQUET` format in the session of `ctx` (see [`register_tparquet_state`]).
pub fn register_tparquet(ctx: &SessionContext) -> Result<()> {
    register_tparquet_state(&mut ctx.state_ref().write())
}
