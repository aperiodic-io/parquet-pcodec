//! The integration tests of tparquet-datafusion, in one binary (one link of DataFusion per
//! build, which matters for cargo mutants).

mod common;
mod conformance;
mod corrupt;
mod properties;
mod sql;
mod vectors_cli;
mod write;
