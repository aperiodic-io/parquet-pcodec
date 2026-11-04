//! Runs the SQL tests (`tests/*.py`, pytest on Python `duckdb` 2.0.0) against the extension this
//! very build produced, so that `cargo test` (and every `cargo mutants` mutant) checks the code
//! DuckDB actually loads.
//!
//! The Python environment is `duckdb/.venv` (`uv sync` in `duckdb/`), or the interpreter in
//! `$PCODEC_DUCKDB_PYTHON`; `$PCODEC_DUCKDB_PYTEST_ARGS` adds pytest arguments (the mutation run
//! passes `-m "not slow"`).

use parquet_pcodec_duckdb::metadata::{Metadata, current_platform, package};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The extension library cargo built for this test binary (`target/<profile>/deps/`).
fn library() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let deps = exe.parent().unwrap();
    let name = format!("{}parquet_pcodec_duckdb{}", std::env::consts::DLL_PREFIX, std::env::consts::DLL_SUFFIX);
    for dir in [deps, deps.parent().unwrap()] {
        if dir.join(&name).is_file() {
            return dir.join(&name);
        }
    }
    panic!("{name} not found next to {}", exe.display());
}

fn python(manifest: &Path) -> PathBuf {
    if let Some(p) = std::env::var_os("PCODEC_DUCKDB_PYTHON") {
        return p.into();
    }
    let venv = manifest.join(".venv/bin/python");
    assert!(
        venv.is_file(),
        "no Python environment for the SQL tests: run `uv sync --python 3.14` in duckdb/ or set PCODEC_DUCKDB_PYTHON"
    );
    venv
}

#[test]
fn sql_tests() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let extension = dir.path().join("parquet_pcodec.duckdb_extension");
    let bytes = std::fs::read(library()).unwrap();
    let platform = current_platform().expect("a platform DuckDB names");
    std::fs::write(&extension, package(&bytes, &Metadata::for_platform(platform)).unwrap()).unwrap();

    let mut cmd = Command::new(python(manifest));
    cmd.current_dir(manifest)
        .args(["-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", "tests"])
        .env("PCODEC_DUCKDB_EXTENSION", &extension)
        .env("PCODEC_DUCKDB_SCRATCH", dir.path());
    if let Ok(extra) = std::env::var("PCODEC_DUCKDB_PYTEST_ARGS") {
        cmd.args(extra.split_whitespace());
    }
    let out = cmd.output().expect("run pytest");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "SQL tests failed\n{stdout}\n{stderr}");
    println!("{stdout}");
}
