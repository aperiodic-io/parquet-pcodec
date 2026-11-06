//! Runs the Java test suite of `java/tparquet-jni` against the library this build produced, so
//! that `cargo test -p tparquet-jni` (and `cargo mutants` on this crate) tests the native
//! methods through Java.
//!
//! `TPARQUET_MAVEN` names the `mvn` executable; the suite runs offline (`-o`), so the Maven
//! repository must already hold its dependencies (`make -C java test` once). Without it the
//! test only says so and passes, so that `cargo test --workspace` works on machines without
//! Java; `make -C java jni-mutants` and the CI set it.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The library this test run built: `target/<profile>/deps/libtparquet_jni.so`, next to the
/// test binary (`cargo test` builds the library for the test, but does not copy it up).
fn library() -> PathBuf {
    let exe = std::env::current_exe().expect("test binary path");
    let deps = exe.parent().expect("target/<profile>/deps/<test>");
    deps.join(format!("{}tparquet_jni{}", std::env::consts::DLL_PREFIX, std::env::consts::DLL_SUFFIX))
}

#[test]
fn java() {
    let Some(mvn) = std::env::var_os("TPARQUET_MAVEN") else {
        eprintln!("TPARQUET_MAVEN is not set: the Java suite did not run");
        return;
    };
    let lib = library();
    assert!(lib.exists(), "{} was not built", lib.display());
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let java = manifest.join("..").join("..").join("java");
    let status = Command::new(mvn)
        .args(["-o", "-q", "-B", "-pl", "tparquet-jni", "test"])
        .arg(format!("-Dtparquet.jni.library={}", lib.display()))
        .arg(format!("-Dtparquet.testdata={}", manifest.join("..").join("..").join("testdata").display()))
        .current_dir(&java)
        .status()
        .expect("run mvn");
    assert!(status.success(), "the Java suite failed: {status}");
}
