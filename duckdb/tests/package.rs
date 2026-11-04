//! The `package-extension` binary.

use parquet_pcodec_duckdb::metadata::{Metadata, current_platform};
use std::process::Command;

fn run(args: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_package-extension")).args(args).output().unwrap()
}

#[test]
fn appends_the_footer_for_this_or_a_named_platform() {
    let dir = tempfile::tempdir().unwrap();
    let lib = dir.path().join("lib.so");
    std::fs::write(&lib, b"\x7fELF not really").unwrap();
    let out = dir.path().join("parquet_pcodec.duckdb_extension");
    let r = run(&[lib.as_os_str(), out.as_os_str()]);
    assert!(r.status.success(), "{}", String::from_utf8_lossy(&r.stderr));
    let file = std::fs::read(&out).unwrap();
    assert!(file.starts_with(b"\x7fELF not really"));
    assert_eq!(Metadata::parse(&file), Some(Metadata::for_platform(current_platform().unwrap())));

    let r = run(&[lib.as_os_str(), out.as_os_str(), "osx_arm64".as_ref()]);
    assert!(r.status.success());
    assert_eq!(Metadata::parse(&std::fs::read(&out).unwrap()).unwrap().platform, "osx_arm64");
}

#[test]
fn explains_its_errors() {
    let dir = tempfile::tempdir().unwrap();
    let r = run(&[]);
    assert_eq!(r.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&r.stderr),
        "package-extension: usage: package-extension LIBRARY OUTPUT [PLATFORM]\n"
    );
    let missing = dir.path().join("missing.so");
    let r = run(&[missing.as_os_str(), dir.path().join("x").as_os_str()]);
    assert!(!r.status.success());
    assert!(
        String::from_utf8_lossy(&r.stderr)
            .starts_with(&format!("package-extension: cannot read {}", missing.display()))
    );
    let lib = dir.path().join("lib.so");
    std::fs::write(&lib, b"lib").unwrap();
    let r = run(&[lib.as_os_str(), dir.path().join("no/such/dir/x").as_os_str()]);
    assert!(String::from_utf8_lossy(&r.stderr).starts_with("package-extension: cannot write "));
    let r = run(&[lib.as_os_str(), dir.path().join("x").as_os_str(), "a-platform-name-longer-than-32-bytes".as_ref()]);
    assert!(String::from_utf8_lossy(&r.stderr).contains("must be at most 32 ASCII bytes"));
}
