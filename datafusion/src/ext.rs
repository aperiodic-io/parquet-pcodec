//! Extension traits for [`SessionContext`] and [`DataFrame`].

use crate::format::TparquetFormatFactory;
use crate::options::TparquetOptions;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::file_format::format_as_file_type;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionContext};
use std::collections::HashMap;
use std::sync::Arc;

/// Reading TPARQUET files from a [`SessionContext`].
///
/// With the fork, these are `read_parquet` and `register_parquet`: they are here so that code
/// says what it reads.
#[async_trait]
pub trait SessionContextExt {
    /// Reads the files at `path` (a file, a directory, a glob) as a [`DataFrame`].
    async fn read_tparquet(&self, path: &str, options: ParquetReadOptions<'_>) -> Result<DataFrame>;

    /// Registers the files at `path` as the table `name`.
    async fn register_tparquet(&self, name: &str, path: &str, options: ParquetReadOptions<'_>) -> Result<()>;
}

#[async_trait]
impl SessionContextExt for SessionContext {
    async fn read_tparquet(&self, path: &str, options: ParquetReadOptions<'_>) -> Result<DataFrame> {
        self.read_parquet(path, options).await
    }

    async fn register_tparquet(&self, name: &str, path: &str, options: ParquetReadOptions<'_>) -> Result<()> {
        self.register_parquet(name, path, options).await
    }
}

/// Writing a [`DataFrame`] as TPARQUET.
#[async_trait]
pub trait DataFrameExt {
    /// Writes the frame to `path` (a file when it has an extension, else a directory of
    /// files) with PCODEC as `options` say; `parquet` replaces the session's Parquet options.
    /// Returns the batch with the number of rows written, as `write_parquet` does.
    async fn write_tparquet(
        self,
        path: &str,
        options: TparquetOptions,
        parquet: Option<TableParquetOptions>,
    ) -> Result<Vec<RecordBatch>>;
}

#[async_trait]
impl DataFrameExt for DataFrame {
    async fn write_tparquet(
        self,
        path: &str,
        options: TparquetOptions,
        parquet: Option<TableParquetOptions>,
    ) -> Result<Vec<RecordBatch>> {
        let mut factory = TparquetFormatFactory::new_with_options(options);
        factory.parquet = parquet;
        let (state, plan) = self.into_parts();
        let plan = LogicalPlanBuilder::copy_to(
            plan,
            path.into(),
            format_as_file_type(Arc::new(factory)),
            HashMap::new(),
            vec![],
        )?
        .build()?;
        DataFrame::new(state, plan).collect().await
    }
}
