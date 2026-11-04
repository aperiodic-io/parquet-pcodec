"""PCODEC Parquet files from Python.

PCODEC is a Parquet compression codec (`CompressionCodec` 20547) that stores numeric columns
with pcodec. This package reads any Parquet file, PCODEC or not, and writes PCODEC files,
choosing the codec per column. Data goes in and out through the Arrow PyCapsule interface, so
pyarrow, Polars, DuckDB and nanoarrow objects all work; pyarrow is needed only by the
functions that return pyarrow objects.

Integrations: `parquet_pcodec.pandas`, `parquet_pcodec.polars`, `parquet_pcodec.duckdb`.
"""

from parquet_pcodec._core import (
    ArrowArrayExportable,
    ArrowData,
    ArrowSchemaExportable,
    ArrowStreamExportable,
    FileMetadata,
    ParquetFile,
    Path,
    PcodecError,
    Policy,
    Readable,
    Sink,
    Source,
    Writable,
    Writer,
    inspect,
    read_batches,
    read_table,
    transcode,
    verify,
    write_table,
)
from parquet_pcodec._lib import RecordBatchStream, __version__

__all__ = [
    "ArrowArrayExportable",
    "ArrowData",
    "ArrowSchemaExportable",
    "ArrowStreamExportable",
    "FileMetadata",
    "ParquetFile",
    "Path",
    "PcodecError",
    "Policy",
    "Readable",
    "RecordBatchStream",
    "Sink",
    "Source",
    "Writable",
    "Writer",
    "__version__",
    "inspect",
    "read_batches",
    "read_table",
    "transcode",
    "verify",
    "write_table",
]
