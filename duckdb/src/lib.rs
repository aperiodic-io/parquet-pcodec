//! `parquet_pcodec`: a DuckDB extension that reads and writes Parquet files whose column chunks
//! use the PCODEC codec (`spec/FORMAT.md`), built on DuckDB's C extension API.

pub mod metadata;
pub mod scan;
pub mod types;
