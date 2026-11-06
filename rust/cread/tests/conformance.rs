//! Every conformance vector in `testdata/files` read through the C stream gives exactly its
//! expected rows, and the footer helpers agree with `testdata/files/manifest.json`.

mod common;

use arrow_array::RecordBatchReader;
use common::*;
use serde_json::Value;
use std::path::PathBuf;

fn manifest() -> Vec<Value> {
    let m: Value = serde_json::from_slice(&std::fs::read(testdata().join("files/manifest.json")).unwrap()).unwrap();
    m["files"].as_array().unwrap().clone()
}

fn files() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(testdata().join("files"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    out.sort();
    out
}

#[test]
fn every_vector_reads_as_expected() {
    let files = files();
    assert_eq!(files.len(), manifest().len());
    assert!(files.len() >= 13);
    for file in files {
        let name = file.file_stem().unwrap().to_str().unwrap();
        let want = std::fs::read_to_string(testdata().join("expected").join(format!("{name}.jsonl"))).unwrap();
        let got = jsonl(&read_all(&file).unwrap());
        assert!(got == want, "{name}: rows differ from expected/{name}.jsonl");
    }
}

#[test]
fn every_vector_reads_as_expected_in_small_batches() {
    for file in files() {
        let name = file.file_stem().unwrap().to_str().unwrap();
        let want = std::fs::read_to_string(testdata().join("expected").join(format!("{name}.jsonl"))).unwrap();
        let batches: Vec<_> = open(&file, None, 333).unwrap().collect::<Result<_, _>>().unwrap();
        assert!(batches.iter().all(|b| b.num_rows() <= 333), "{name}");
        assert!(jsonl(&batches) == want, "{name}");
    }
}

#[test]
fn file_info_matches_the_manifest() {
    for entry in manifest() {
        let path = testdata().join(entry["file"].as_str().unwrap());
        let (rows, groups) = file_info(&path).unwrap();
        assert_eq!(rows, entry["rows"].as_i64().unwrap(), "{}", path.display());
        assert_eq!(groups, entry["row_groups"].as_i64().unwrap(), "{}", path.display());
    }
}

#[test]
fn schema_carries_the_footer_key() {
    for entry in manifest() {
        let path = testdata().join(entry["file"].as_str().unwrap());
        let schema = file_schema(&path).unwrap();
        let stream_schema = open(&path, None, 0).unwrap().schema();
        assert_eq!(&schema, stream_schema.as_ref(), "{}", path.display());
        assert_eq!(
            schema.metadata().get("parquet.pcodec.version").map(String::as_str),
            entry["pcodec_version"].as_str(),
            "{}",
            path.display()
        );
    }
}
