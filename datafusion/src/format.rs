//! The TPARQUET file format: DataFusion's Parquet format for reading (the fork reads PCODEC),
//! and [`TparquetSink`] for writing.

use crate::options::TparquetOptions;
use crate::sink::TparquetSink;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::{GetExt, Statistics, not_impl_err};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::{ParquetFormat, ParquetFormatFactory};
use datafusion::datasource::file_format::write::get_writer_schema;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory, FileMeta};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, LexRequirement};
use datafusion::physical_plan::ExecutionPlan;
use object_store::{ObjectMeta, ObjectStore};
use parquet::file::metadata::SortingColumn;
use std::collections::HashMap;
use std::sync::Arc;

/// The name of the format: `STORED AS TPARQUET`.
pub const TPARQUET: &str = "tparquet";

/// Creates [`TparquetFormat`]s for `CREATE EXTERNAL TABLE ... STORED AS TPARQUET` and
/// `COPY ... TO ... STORED AS TPARQUET`.
///
/// Its options are DataFusion's Parquet options (`format.compression`, ...) plus
/// `format.policy`, `format.policy::<column>` and `format.pcodec_level`
/// ([`TparquetOptions::apply`]).
#[derive(Debug, Default)]
pub struct TparquetFormatFactory {
    /// The Parquet options; `None` takes the session's (as [`ParquetFormatFactory`] does).
    pub parquet: Option<TableParquetOptions>,
    /// The TPARQUET options before the statement's own.
    pub options: TparquetOptions,
}

impl TparquetFormatFactory {
    /// A factory with the default options.
    pub fn new() -> Self {
        <Self as Default>::default()
    }

    /// A factory with `options` as its defaults.
    pub fn new_with_options(options: TparquetOptions) -> Self {
        Self { parquet: None, options }
    }

    /// Uses `parquet` instead of the session's Parquet options.
    pub fn with_parquet_options(mut self, parquet: TableParquetOptions) -> Self {
        self.parquet = Some(parquet);
        self
    }
}

impl FileFormatFactory for TparquetFormatFactory {
    fn create(&self, state: &dyn Session, format_options: &HashMap<String, String>) -> Result<Arc<dyn FileFormat>> {
        let mut options = self.options.clone();
        let rest = options.apply(format_options)?;
        let parquet = ParquetFormatFactory { options: self.parquet.clone() }.create(state, &rest)?;
        let parquet = parquet
            .downcast_ref::<ParquetFormat>()
            .ok_or_else(|| DataFusionError::Internal("ParquetFormatFactory made another format".into()))?;
        Ok(Arc::new(TparquetFormat::new(ParquetFormat::default().with_options(parquet.options().clone()), options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(TparquetFormat::default())
    }
}

impl GetExt for TparquetFormatFactory {
    /// The name `STORED AS` uses; the files themselves are `.parquet`.
    fn get_ext(&self) -> String {
        TPARQUET.to_string()
    }
}

/// Parquet files with the PCODEC codec: reads as DataFusion's [`ParquetFormat`] (pruning,
/// page index, filter pushdown and projection included), writes with [`TparquetSink`].
#[derive(Debug, Default)]
pub struct TparquetFormat {
    parquet: ParquetFormat,
    options: TparquetOptions,
}

impl TparquetFormat {
    /// The format with DataFusion's Parquet format `parquet` and the TPARQUET `options`.
    pub fn new(parquet: ParquetFormat, options: TparquetOptions) -> Self {
        Self { parquet, options }
    }

    /// The Parquet format it reads with.
    pub fn parquet(&self) -> &ParquetFormat {
        &self.parquet
    }

    /// The TPARQUET options it writes with.
    pub fn options(&self) -> &TparquetOptions {
        &self.options
    }
}

#[async_trait]
impl FileFormat for TparquetFormat {
    fn get_ext(&self) -> String {
        self.parquet.get_ext()
    }

    fn get_ext_with_compression(&self, file_compression_type: &FileCompressionType) -> Result<String> {
        self.parquet.get_ext_with_compression(file_compression_type)
    }

    /// `UNCOMPRESSED` (Parquet's own is `None`, which means the same: Parquet files are never
    /// compressed as a whole). With `Some`, `CREATE EXTERNAL TABLE ... STORED AS TPARQUET` on a
    /// directory lists `*.parquet` (the extension of the format) instead of `*.tparquet` (the
    /// name after `STORED AS`).
    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(FileCompressionType::UNCOMPRESSED)
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.parquet.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.parquet.infer_stats(state, store, table_schema, object).await
    }

    async fn infer_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Option<LexOrdering>> {
        self.parquet.infer_ordering(state, store, table_schema, object).await
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<FileMeta> {
        self.parquet.infer_stats_and_ordering(state, store, table_schema, object).await
    }

    async fn create_physical_plan(&self, state: &dyn Session, conf: FileScanConfig) -> Result<Arc<dyn ExecutionPlan>> {
        self.parquet.create_physical_plan(state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for TPARQUET");
        }
        // as DataFusion's Parquet format: an ordering that is not made of plain columns (such
        // as `ORDER BY random()`) is left out of the file metadata
        let sorting_columns = order_requirements
            .as_ref()
            .and_then(|r| sorting_columns(&r.clone().into(), conf.output_schema(), &get_writer_schema(&conf)).ok())
            .filter(|c| !c.is_empty());
        let sink = TparquetSink::try_new(conf, self.parquet.options().clone(), &self.options, sorting_columns)?;
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), order_requirements)))
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.parquet.file_source(table_schema)
    }
}

/// The Parquet sorting columns of `ordering` (as DataFusion's Parquet sink writes them):
/// columns of the input missing from the file (partition columns) are left out; an
/// expression that is not a plain column is an error.
pub(crate) fn sorting_columns(
    ordering: &LexOrdering,
    input_schema: &Schema,
    writer_schema: &Schema,
) -> Result<Vec<SortingColumn>> {
    let mut out = vec![];
    for sort in ordering.iter() {
        let Some(column) = sort.expr.downcast_ref::<Column>() else {
            return Err(DataFusionError::Plan(format!(
                "Parquet sorting_columns only supports simple column references, but got expression: {}",
                sort.expr
            )));
        };
        let field = input_schema.fields().get(column.index()).ok_or_else(|| {
            DataFusionError::Internal(format!("sorting column {} is not in the input schema", column.name()))
        })?;
        if let Some((index, _)) = writer_schema.column_with_name(field.name()) {
            out.push(SortingColumn {
                column_idx: i32::try_from(index).map_err(|e| DataFusionError::Plan(e.to_string()))?,
                descending: sort.options.descending,
                nulls_first: sort.options.nulls_first,
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, lit};

    fn sort(
        expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        descending: bool,
        nulls_first: bool,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr::new(expr, SortOptions { descending, nulls_first })
    }

    fn col(name: &str, i: usize) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
        Arc::new(Column::new(name, i))
    }

    #[test]
    fn sorting_columns_follow_the_writer_schema() {
        let input = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("part", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let writer =
            Schema::new(vec![Field::new("a", DataType::Int64, false), Field::new("b", DataType::Int64, false)]);
        let ordering = LexOrdering::new([
            sort(col("part", 1), false, true),
            sort(col("b", 2), true, false),
            sort(col("a", 0), false, true),
        ])
        .unwrap();
        let got: Vec<(i32, bool, bool)> = sorting_columns(&ordering, &input, &writer)
            .unwrap()
            .iter()
            .map(|c| (c.column_idx, c.descending, c.nulls_first))
            .collect();
        assert_eq!(got, [(1, true, false), (0, false, true)]);

        let expr = Arc::new(BinaryExpr::new(col("a", 0), Operator::Plus, lit(1i64)));
        let ordering = LexOrdering::new([sort(expr, false, true)]).unwrap();
        let err = sorting_columns(&ordering, &input, &writer).unwrap_err().to_string();
        assert!(err.contains("only supports simple column references, but got expression: a@0 + 1"), "{err}");
        let ordering = LexOrdering::new([sort(col("z", 7), false, true)]).unwrap();
        let err = sorting_columns(&ordering, &input, &writer).unwrap_err().to_string();
        assert!(err.contains("sorting column z is not in the input schema"), "{err}");
    }

    #[test]
    fn the_format_reads_as_parquet() {
        let f = TparquetFormat::default();
        assert_eq!(f.get_ext(), "parquet");
        assert_eq!(f.compression_type().map(|c| c.is_compressed()), Some(false));
        assert_eq!(f.get_ext_with_compression(&FileCompressionType::UNCOMPRESSED).unwrap(), "parquet");
        assert!(f.get_ext_with_compression(&FileCompressionType::GZIP).is_err());
        assert_eq!(f.options(), &TparquetOptions::default());
        assert_eq!(TparquetFormatFactory::new().get_ext(), "tparquet");
        let factory = TparquetFormatFactory::new_with_options(TparquetOptions::default().with_level(1).unwrap())
            .with_parquet_options(TableParquetOptions::default());
        assert_eq!(factory.options.level.compression_level(), 1);
        assert!(factory.parquet.is_some());
        assert!(factory.default().downcast_ref::<TparquetFormat>().is_some());
    }
}
