//! [`TparquetSink`]: DataFusion's Parquet sink with PCODEC for the numeric columns.
//!
//! DataFusion's own `ParquetSink` builds its writer properties from `TableParquetOptions`,
//! whose compression strings DataFusion parses itself (and refuses `pcodec`). This sink
//! builds the same properties, then sets PCODEC (`PLAIN`, no dictionary, V2 pages) on the
//! columns the [`TparquetOptions`] select, and writes each file with the fork's
//! `AsyncArrowWriter`.

use crate::codecs::Codecs;
use crate::options::TparquetOptions;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::not_impl_err;
use datafusion::common::runtime::{JoinSet, SpawnedTask};
use datafusion::datasource::file_format::write::demux::DemuxedStreamReceiver;
use datafusion::datasource::file_format::write::get_writer_schema;
use datafusion::datasource::physical_plan::{FileSink, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use datafusion_datasource::display::FileGroupDisplay;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::file::metadata::{ParquetMetaData, SortingColumn};
use parquet::file::properties::WriterPropertiesBuilder;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Writes the files of a `COPY ... STORED AS TPARQUET` or an `INSERT INTO` a TPARQUET table.
pub struct TparquetSink {
    config: FileSinkConfig,
    parquet_options: TableParquetOptions,
    codecs: Arc<Codecs>,
    /// The metadata of every file written, by path.
    written: Arc<Mutex<HashMap<Path, ParquetMetaData>>>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for TparquetSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TparquetSink").finish()
    }
}

impl DisplayAs for TparquetSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TparquetSink(file_groups=")?;
        FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
        write!(f, ")")
    }
}

impl TparquetSink {
    /// A sink writing `config` with DataFusion's `parquet_options`, PCODEC as `options` say,
    /// and `sorting_columns` in the metadata. Fails, before anything is written, on options
    /// the sink cannot honour: encryption, and a per-column policy for a column the output
    /// does not have.
    pub fn try_new(
        config: FileSinkConfig,
        parquet_options: TableParquetOptions,
        options: &TparquetOptions,
        sorting_columns: Option<Vec<SortingColumn>>,
    ) -> Result<Self> {
        if parquet_options.crypto.file_encryption.is_some() || parquet_options.crypto.factory_id.is_some() {
            return not_impl_err!("TPARQUET does not write encrypted files");
        }
        let schema = get_writer_schema(&config);
        let mut table_options = parquet_options.clone();
        if !table_options.global.skip_arrow_metadata {
            table_options.arrow_schema(&schema);
        }
        let base = WriterPropertiesBuilder::try_from(&table_options)?.set_sorting_columns(sorting_columns).build();
        let codecs = Arc::new(Codecs::try_new(schema, base, options)?);
        Ok(Self {
            config,
            parquet_options,
            codecs,
            written: Default::default(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The metadata of the files written so far, by path.
    pub fn written(&self) -> HashMap<Path, ParquetMetaData> {
        self.written.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Writes the batches of `rx` to `path`; the first batch with rows decides the `Auto` columns.
async fn write_file(
    codecs: Arc<Codecs>,
    skip_arrow_metadata: bool,
    store: Arc<dyn ObjectStore>,
    path: Path,
    mut rx: tokio::sync::mpsc::Receiver<RecordBatch>,
    context: Arc<TaskContext>,
) -> Result<(Path, ParquetMetaData)> {
    let open = |sample: Option<&RecordBatch>| -> Result<AsyncArrowWriter<BufWriter>> {
        let options = ArrowWriterOptions::new()
            .with_properties(codecs.writer_properties(sample)?)
            .with_skip_arrow_metadata(skip_arrow_metadata);
        let sink = BufWriter::new(Arc::clone(&store), path.clone());
        Ok(AsyncArrowWriter::try_new_with_options(sink, Arc::clone(codecs.schema()), options)?)
    };
    let reservation = MemoryConsumer::new(format!("TparquetSink[{path}]")).register(context.memory_pool());
    let mut writer = None;
    while let Some(batch) = rx.recv().await {
        if batch.num_rows() == 0 {
            continue;
        }
        let w = match &mut writer {
            Some(w) => w,
            None => writer.insert(open(Some(&batch))?),
        };
        w.write(&batch).await?;
        reservation.try_resize(w.memory_size())?;
    }
    let writer = match writer {
        Some(w) => w,
        None => open(None)?,
    };
    let metadata = writer.close().await?;
    Ok((path, metadata))
}

#[async_trait]
impl FileSink for TparquetSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let rows_written = MetricBuilder::new(&self.metrics).global_counter("rows_written");
        let bytes_written = MetricBuilder::new(&self.metrics).global_counter("bytes_written");
        let mut tasks: JoinSet<Result<(Path, ParquetMetaData)>> = JoinSet::new();
        while let Some((path, rx)) = file_stream_rx.recv().await {
            tasks.spawn(write_file(
                Arc::clone(&self.codecs),
                self.parquet_options.global.skip_arrow_metadata,
                Arc::clone(&object_store),
                path,
                rx,
                Arc::clone(context),
            ));
        }
        while let Some(result) = tasks.join_next().await {
            let (path, metadata) = match result {
                Ok(r) => r?,
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => return Err(DataFusionError::ExecutionJoin(Box::new(e))),
            };
            rows_written.add(metadata.file_metadata().num_rows() as usize);
            bytes_written.add(metadata.row_groups().iter().map(|rg| rg.compressed_size() as usize).sum());
            self.written.lock().unwrap_or_else(|e| e.into_inner()).insert(path, metadata);
        }
        demux_task.join_unwind().await.map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
        Ok(rows_written.value() as u64)
    }
}

#[async_trait]
impl DataSink for TparquetSink {
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(&self, data: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}
