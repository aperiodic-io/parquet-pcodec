//! The conformance vectors in `testdata/`: up to date, and read correctly.

use assert_cmd::Command;
use parquet_pcodec::canonical;
use parquet_pcodec::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata")
}

fn manifest(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

/// Relative paths of every file under `dir`, but the README.
fn tree(dir: &Path) -> BTreeSet<PathBuf> {
    let mut out = BTreeSet::new();
    for sub in ["files", "expected", "payloads"] {
        for e in std::fs::read_dir(dir.join(sub)).unwrap() {
            out.insert(Path::new(sub).join(e.unwrap().file_name()));
        }
    }
    out
}

#[test]
fn generator_reproduces_testdata_byte_for_byte() {
    let dir = tempfile::tempdir().unwrap();
    Command::cargo_bin("gen-testdata").unwrap().arg("--out").arg(dir.path()).assert().success();
    let (ours, committed) = (tree(dir.path()), tree(&testdata()));
    assert_eq!(ours, committed);
    for f in &ours {
        let a = std::fs::read(dir.path().join(f)).unwrap();
        let b = std::fs::read(testdata().join(f)).unwrap();
        assert!(
            a == b,
            "{} differs from the committed vector: run `cargo run -p parquet-pcodec --bin gen-testdata`",
            f.display()
        );
    }
    // a second run over the first replaces everything with the same bytes
    Command::cargo_bin("gen-testdata").unwrap().arg("--out").arg(dir.path()).assert().success();
    assert_eq!(tree(dir.path()), committed);
}

#[test]
fn generator_usage_errors() {
    Command::cargo_bin("gen-testdata")
        .unwrap()
        .arg("--frob")
        .assert()
        .code(1)
        .stderr("gen-testdata: usage: gen-testdata [--out DIR]\n");
    let file = tempfile::NamedTempFile::new().unwrap();
    Command::cargo_bin("gen-testdata")
        .unwrap()
        .arg("--out")
        .arg(file.path())
        .assert()
        .code(1)
        .stderr(predicates::str::starts_with("gen-testdata: "));
}

#[test]
fn files_read_as_expected() {
    let m = manifest(&testdata().join("files/manifest.json"));
    let files = m["files"].as_array().unwrap();
    assert_eq!(files.len(), 13);
    let mut modes = BTreeSet::new();
    let mut pcodec = 0;
    for f in files {
        let path = testdata().join(f["file"].as_str().unwrap());
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap().build().unwrap();
        let mut actual = String::new();
        for b in reader {
            canonical::write_batch(&b.unwrap(), &mut actual).unwrap();
        }
        let expected = std::fs::read_to_string(testdata().join(f["expected"].as_str().unwrap())).unwrap();
        assert!(actual == expected, "{}", f["name"]);
        assert_eq!(expected.lines().count() as i64, f["rows"].as_i64().unwrap());
        for c in f["columns"].as_array().unwrap() {
            modes.extend(c["modes"].as_object().unwrap().keys().cloned());
        }
        pcodec += usize::from(f["pcodec_version"] == "1");
    }
    for m in ["RAW", "PCO", "DECIMAL", "ZSTD"] {
        assert!(modes.contains(m), "{m}");
    }
    assert_eq!(pcodec, 12);
}

#[test]
fn payload_vectors_decode_or_fail() {
    let dir = testdata().join("payloads");
    let m = manifest(&dir.join("manifest.json"));
    let (mut valid, mut invalid) = (0, 0);
    for v in m["vectors"].as_array().unwrap() {
        let payload = std::fs::read(dir.join(v["payload"].as_str().unwrap())).unwrap();
        let len = v["expected_len"].as_u64().unwrap() as usize;
        let got = parquet_pcodec_core::decompress(&payload, len);
        if v["valid"] == true {
            let decoded = std::fs::read(dir.join(v["decoded"].as_str().unwrap())).unwrap();
            assert_eq!(got.unwrap(), decoded, "{}", v["name"]);
            let mode = parquet_pcodec_core::mode_of(&payload).unwrap();
            assert_eq!(format!("{mode:?}").to_uppercase(), v["mode"].as_str().unwrap(), "{}", v["name"]);
            valid += 1;
        } else {
            assert_eq!(got.unwrap_err().to_string(), v["error"].as_str().unwrap(), "{}", v["name"]);
            invalid += 1;
        }
    }
    assert_eq!((valid, invalid), (14, 21));
}
