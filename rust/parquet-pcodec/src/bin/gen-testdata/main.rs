//! Writes the conformance vectors in `testdata/` (see `testdata/README.md`):
//! `files/<name>.parquet`, `expected/<name>.jsonl` and `payloads/`. The output is the same
//! byte for byte on every run.
//!
//! Usage: `gen-testdata [--out DIR]` (default: the repository's `testdata/`).
//!
//! `gen-testdata --check DIR` instead reads every `DIR/files/<name>.parquet` and compares its
//! canonical rows with `DIR/expected/<name>.jsonl`: the Rust reader's half of the
//! cross-language acceptance tests, for vectors other implementations wrote.

mod cases;
mod payloads;
mod rng;

use parquet_pcodec::canonical;
use parquet_pcodec::inspect::inspect;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

type Result<T> = std::result::Result<T, String>;

fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    std::fs::write(path, bytes).map_err(|e| format!("{}: {e}", path.display()))
}

fn json_bytes(v: &Value) -> Vec<u8> {
    let mut s = serde_json::to_string_pretty(v).expect("JSON value");
    s.push('\n');
    s.into_bytes()
}

/// Empties and recreates `dir/name`.
fn fresh(dir: &Path, name: &str) -> Result<PathBuf> {
    let d = dir.join(name);
    if d.exists() {
        std::fs::remove_dir_all(&d).map_err(|e| format!("{}: {e}", d.display()))?;
    }
    std::fs::create_dir_all(&d).map_err(|e| format!("{}: {e}", d.display()))?;
    Ok(d)
}

fn files(out: &Path) -> Result<()> {
    let files = fresh(out, "files")?;
    let expected = fresh(out, "expected")?;
    let mut manifest = vec![];
    let mut modes = BTreeSet::new();
    for case in cases::all() {
        let bytes = cases::write(&case).map_err(|e| format!("{}: {e}", case.name))?;
        let mut lines = String::new();
        for b in &case.batches {
            canonical::write_batch(b, &mut lines).map_err(|e| format!("{}: {e}", case.name))?;
        }
        let report = inspect(&bytes::Bytes::from(bytes.clone())).map_err(|e| format!("{}: {e}", case.name))?;
        for c in &report.columns {
            modes.extend(c.modes.keys().cloned());
        }
        manifest.push(json!({
            "name": case.name,
            "description": case.description,
            "file": format!("files/{}.parquet", case.name),
            "expected": format!("expected/{}.jsonl", case.name),
            "rows": report.rows,
            "row_groups": report.row_groups,
            "pcodec_version": report.pcodec_version,
            "columns": report.columns.iter().map(|c| json!({
                "path": c.path,
                "physical_type": c.physical_type,
                "codecs": c.codecs,
                "pages": c.pages,
                "modes": c.modes,
            })).collect::<Vec<_>>(),
        }));
        write(&files.join(format!("{}.parquet", case.name)), &bytes)?;
        write(&expected.join(format!("{}.jsonl", case.name)), lines.as_bytes())?;
    }
    for m in ["RAW", "PCO", "DECIMAL", "ZSTD"] {
        if !modes.contains(m) {
            return Err(format!("no file has a {m} payload"));
        }
    }
    write(&files.join("manifest.json"), &json_bytes(&json!({ "files": manifest })))
}

fn payloads(out: &Path) -> Result<()> {
    let dir = fresh(out, "payloads")?;
    let mut manifest = vec![];
    for v in payloads::all() {
        let mut entry = json!({
            "name": v.name,
            "payload": format!("{}.bin", v.name),
            "expected_len": v.expected_len,
            "valid": v.decoded.is_some(),
        });
        let fields = entry.as_object_mut().expect("object");
        fields.extend(v.meta.as_object().expect("object").clone());
        if let Some(decoded) = &v.decoded {
            fields.insert("decoded".into(), format!("{}.out", v.name).into());
            write(&dir.join(format!("{}.out", v.name)), decoded)?;
        }
        write(&dir.join(format!("{}.bin", v.name)), &v.payload)?;
        manifest.push(entry);
    }
    write(&dir.join("manifest.json"), &json_bytes(&json!({ "format_version": 1, "vectors": manifest })))
}

/// The canonical rows of the Parquet file at `path`.
fn canonical_rows(path: &Path) -> Result<String> {
    let builder = parquet_pcodec::read::open(path).map_err(|e| e.to_string())?;
    let reader = builder.build().map_err(|e| e.to_string())?;
    let mut rows = String::new();
    for batch in reader {
        canonical::write_batch(&batch.map_err(|e| e.to_string())?, &mut rows).map_err(|e| e.to_string())?;
    }
    Ok(rows)
}

/// Checks every vector in `dir` (see the module docs) and returns how many there are.
fn check(dir: &Path) -> Result<usize> {
    let files = dir.join("files");
    let entries = std::fs::read_dir(&files).map_err(|e| format!("{}: {e}", files.display()))?;
    let mut names = BTreeSet::new();
    for e in entries {
        let path = e.map_err(|e| format!("{}: {e}", files.display()))?.path();
        if path.extension().is_some_and(|x| x == "parquet") {
            names.insert(path.file_stem().expect("a file name").to_string_lossy().into_owned());
        }
    }
    if names.is_empty() {
        return Err(format!("no files/*.parquet in {}", dir.display()));
    }
    for name in &names {
        let fail = |e: String| format!("{name}: {e}");
        let expected = format!("expected/{name}.jsonl");
        let want = std::fs::read_to_string(dir.join(&expected)).map_err(|e| fail(format!("{expected}: {e}")))?;
        let got = canonical_rows(&files.join(format!("{name}.parquet"))).map_err(fail)?;
        if let Some(i) =
            (0..got.lines().count().max(want.lines().count())).find(|&i| got.lines().nth(i) != want.lines().nth(i))
        {
            return Err(fail(format!("line {} differs from {expected}", i + 1)));
        }
    }
    Ok(names.len())
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let out = match args.as_slice() {
        [] => Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata"),
        [flag, dir] if flag == "--out" => PathBuf::from(dir),
        [flag, dir] if flag == "--check" => {
            println!("{} vectors match", check(Path::new(dir))?);
            return Ok(());
        }
        _ => return Err("usage: gen-testdata [--out DIR | --check DIR]".into()),
    };
    files(&out)?;
    payloads(&out)
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gen-testdata: {e}");
            ExitCode::FAILURE
        }
    }
}
