//! Runs the Python test suite against the extension module this build produced, so that
//! `cargo test -p parquet-pcodec-python` (and `cargo mutants` on this crate) tests the Rust
//! code through Python.
//!
//! `PARQUET_PCODEC_PYTHON` names an interpreter with the development dependencies installed
//! (`python/.venv/bin/python` after `uv sync`). Without it the test only says so and passes,
//! so that `cargo test --workspace` works on machines without that environment; `make
//! python-mutants` and the CI set it. `PARQUET_PCODEC_PYTEST_ARGS` replaces the default pytest
//! selection (`-m "not slow"`).

use std::path::{Path, PathBuf};
use std::process::Command;

/// The extension this test run built: `target/<profile>/deps/lib_lib.so`, next to the test
/// binary (`cargo test` builds the library for the test, but does not copy it up).
fn extension() -> PathBuf {
    let exe = std::env::current_exe().expect("test binary path");
    let deps = exe.parent().expect("target/<profile>/deps/<test>");
    deps.join(format!("{}_lib{}", std::env::consts::DLL_PREFIX, std::env::consts::DLL_SUFFIX))
}

/// A copy of the Python package with `lib` as its extension module.
fn package(lib: &Path) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("pytest-{}", std::process::id()));
    let pkg = root.join("parquet_pcodec");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&pkg).expect("create the package directory");
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("parquet_pcodec");
    for entry in std::fs::read_dir(&src).expect("read the package sources") {
        let path = entry.expect("package entry").path();
        if path.is_file() && path.extension().is_some_and(|e| e == "py" || e == "pyi" || e == "typed") {
            std::fs::copy(&path, pkg.join(path.file_name().expect("file name"))).expect("copy a source");
        }
    }
    std::fs::copy(lib, pkg.join("_lib.abi3.so")).expect("copy the extension");
    root
}

#[test]
fn pytest() {
    let Some(python) = std::env::var_os("PARQUET_PCODEC_PYTHON") else {
        eprintln!("PARQUET_PCODEC_PYTHON is not set: the Python suite did not run");
        return;
    };
    let lib = extension();
    assert!(lib.exists(), "{} was not built", lib.display());
    let root = package(&lib);
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let args: Vec<String> = match std::env::var("PARQUET_PCODEC_PYTEST_ARGS") {
        Ok(a) => a.split_whitespace().map(String::from).collect(),
        Err(_) => vec!["-m".into(), "not slow".into()],
    };
    let mut cmd = Command::new(python);
    cmd.args(["-m", "pytest", "-x", "-q", "-p", "no:cacheprovider"])
        .args(&args)
        .arg(manifest.join("tests"))
        .env("PYTHONPATH", &root)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env("PARQUET_PCODEC_TESTDATA", manifest.join("..").join("testdata"))
        .current_dir(manifest);
    let status = cmd.status().expect("run pytest");
    let _ = std::fs::remove_dir_all(&root);
    assert!(status.success(), "pytest failed: {status}");
}
