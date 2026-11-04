//! Runs the SQL tests (`tests/*.py`, pytest on Python `duckdb` 2.0.0) against the extension this
//! very build produced, so that `cargo test` (and every `cargo mutants` mutant) checks the code
//! DuckDB actually loads.
//!
//! The Python environment is `duckdb/.venv` (`uv sync` in `duckdb/`), or the interpreter in
//! `$PPARQUET_DUCKDB_PYTHON`; `$PPARQUET_DUCKDB_PYTEST_MARK` selects tests by marker (the mutation
//! run passes `not slow`).

use pparquet_duckdb::metadata::{Metadata, current_platform, package};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The extension library cargo built for this test binary (`target/<profile>/deps/`).
fn library() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let deps = exe.parent().unwrap();
    let name = format!("{}pparquet_duckdb{}", std::env::consts::DLL_PREFIX, std::env::consts::DLL_SUFFIX);
    for dir in [deps, deps.parent().unwrap()] {
        if dir.join(&name).is_file() {
            return dir.join(&name);
        }
    }
    panic!("{name} not found next to {}", exe.display());
}

fn python(manifest: &Path) -> PathBuf {
    if let Some(p) = std::env::var_os("PPARQUET_DUCKDB_PYTHON") {
        return p.into();
    }
    let venv = manifest.join(".venv/bin/python");
    assert!(
        venv.is_file(),
        "no Python environment for the SQL tests: run `uv sync --python 3.14` in duckdb/ or set PPARQUET_DUCKDB_PYTHON"
    );
    venv
}

/// Builds the `pparquet` CLI (the tests transcode and `verify` with it) into this
/// build's target directory, and returns its path.
fn cli(manifest: &Path) -> PathBuf {
    let profile_dir = library().parent().unwrap().to_path_buf();
    let profile_dir =
        if profile_dir.ends_with("deps") { profile_dir.parent().unwrap().to_path_buf() } else { profile_dir };
    let target = profile_dir.parent().unwrap();
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let mut cmd = Command::new(cargo);
    cmd.current_dir(manifest.parent().unwrap())
        .args(["build", "-q", "-j", "2", "-p", "pparquet", "--bin", "pparquet", "--target-dir"])
        .arg(target);
    if profile_dir.file_name().is_some_and(|p| p == "release") {
        cmd.arg("--release");
    }
    let status = cmd.status().expect("run cargo");
    assert!(status.success(), "cannot build the pparquet CLI");
    profile_dir.join(format!("pparquet{}", std::env::consts::EXE_SUFFIX))
}

#[test]
fn sql_tests() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = tempfile::tempdir().unwrap();
    let extension = dir.path().join("pparquet.duckdb_extension");
    let bytes = std::fs::read(library()).unwrap();
    let platform = current_platform().expect("a platform DuckDB names");
    std::fs::write(&extension, package(&bytes, &Metadata::for_platform(platform)).unwrap()).unwrap();

    let mut cmd = Command::new(python(manifest));
    cmd.current_dir(manifest)
        .args(["-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", "tests"])
        .env("PPARQUET_DUCKDB_EXTENSION", &extension)
        .env("PPARQUET_DUCKDB_SCRATCH", dir.path())
        .env("PPARQUET_CLI", cli(manifest));
    if let Ok(mark) = std::env::var("PPARQUET_DUCKDB_PYTEST_MARK") {
        cmd.args(["-m", &mark]);
    }
    let out = cmd.output().expect("run pytest");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "SQL tests failed\n{stdout}\n{stderr}");
    println!("{stdout}");
}
