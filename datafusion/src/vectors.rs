//! The DataFusion reader and writer of the cross-language acceptance tests
//! (`acceptance/cross/run.sh`), behind the `tparquet-datafusion-vectors` binary.
//!
//! A vector directory holds `files/<name>.parquet` and `expected/<name>.jsonl`, the canonical
//! rows of `testdata/README.md`.
//!
//! - [`check`] reads every `DIR/files/*.parquet` with DataFusion and compares its canonical
//!   rows with `DIR/expected/<name>.jsonl`.
//! - [`write`] rewrites every vector of each source directory with
//!   `COPY ... TO ... STORED AS TPARQUET`, copying the expected rows.

use crate::canonical;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use std::path::{Path, PathBuf};

/// Errors are messages for the user: the binary prints them after its name.
pub type Result<T> = std::result::Result<T, String>;

/// `(name, file, expected)` of every vector in `dir`, by name; a file without expected rows
/// is an error.
pub fn vectors(dir: &Path) -> Result<Vec<(String, PathBuf, PathBuf)>> {
    let files = dir.join("files");
    let entries = std::fs::read_dir(&files).map_err(|e| format!("{}: {e}", files.display()))?;
    let mut out = vec![];
    for e in entries {
        let path = e.map_err(|e| format!("{}: {e}", files.display()))?.path();
        if path.extension().is_some_and(|x| x == "parquet") {
            let name = path.file_stem().unwrap_or_default().to_string_lossy().into_owned();
            let expected = dir.join("expected").join(format!("{name}.jsonl"));
            out.push((name, path, expected));
        }
    }
    if out.is_empty() {
        return Err(format!("no files/*.parquet in {}", dir.display()));
    }
    out.sort();
    for (name, _, expected) in &out {
        if !expected.is_file() {
            return Err(format!("{name}: no expected/{name}.jsonl"));
        }
    }
    Ok(out)
}

/// A session with TPARQUET registered and one target partition, so that every scan and every
/// `COPY` keeps the rows of a file in file order.
pub fn session() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    crate::register_tparquet(&ctx).expect("registering with overwrite cannot fail");
    ctx
}

/// The canonical rows of the Parquet file at `path`, read with DataFusion.
pub async fn canonical_rows(ctx: &SessionContext, path: &Path) -> Result<String> {
    let df = ctx.read_parquet(path.to_string_lossy().as_ref(), ParquetReadOptions::default()).await;
    let batches = df.map_err(|e| e.to_string())?.collect().await.map_err(|e| e.to_string())?;
    let mut rows = String::new();
    for b in &batches {
        canonical::write_batch(b, &mut rows).map_err(|e| e.to_string())?;
    }
    Ok(rows)
}

/// The index of the first line where `a` and `b` differ, a missing line included.
pub fn first_difference(a: &str, b: &str) -> Option<usize> {
    let (a, b): (Vec<&str>, Vec<&str>) = (a.lines().collect(), b.lines().collect());
    (0..a.len().max(b.len())).find(|&i| a.get(i) != b.get(i))
}

/// Checks every vector in `dir` and returns how many there are.
pub async fn check(dir: &Path) -> Result<usize> {
    let ctx = session();
    let vectors = vectors(dir)?;
    for (name, file, expected) in &vectors {
        let want = std::fs::read_to_string(expected).map_err(|e| format!("{name}: {e}"))?;
        let got = canonical_rows(&ctx, file).await.map_err(|e| format!("{name}: {e}"))?;
        if let Some(i) = first_difference(&got, &want) {
            return Err(format!("{name}: line {} differs from expected/{name}.jsonl", i + 1));
        }
    }
    Ok(vectors.len())
}

/// A SQL string literal.
fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Rewrites every vector of `sources` into `out` with `COPY ... STORED AS TPARQUET` and
/// returns how many it wrote. `out/files` and `out/expected` are emptied first; a name met
/// twice keeps the last source's vector.
pub async fn write(out: &Path, sources: &[PathBuf]) -> Result<usize> {
    let mut all = vec![];
    for src in sources {
        all.extend(vectors(src)?);
    }
    for sub in ["files", "expected"] {
        let d = out.join(sub);
        if d.exists() {
            std::fs::remove_dir_all(&d).map_err(|e| format!("{}: {e}", d.display()))?;
        }
        std::fs::create_dir_all(&d).map_err(|e| format!("{}: {e}", d.display()))?;
    }
    let ctx = session();
    for (name, file, expected) in &all {
        let fail = |e: String| format!("{name}: {e}");
        let target = out.join("files").join(format!("{name}.parquet"));
        ctx.register_parquet("src", file.to_string_lossy().as_ref(), ParquetReadOptions::default())
            .await
            .map_err(|e| fail(e.to_string()))?;
        let copy =
            format!("COPY (SELECT * FROM src) TO {} STORED AS TPARQUET", literal(target.to_string_lossy().as_ref()));
        let result = match ctx.sql(&copy).await {
            Ok(df) => df.collect().await.map(drop),
            Err(e) => Err(e),
        };
        ctx.deregister_table("src").map_err(|e| fail(e.to_string()))?;
        result.map_err(|e| fail(e.to_string()))?;
        let copied = out.join("expected").join(format!("{name}.jsonl"));
        std::fs::copy(expected, &copied).map_err(|e| fail(e.to_string()))?;
    }
    Ok(all.len())
}

/// The command line: `check DIR` or `write OUT SRC...`. Returns the exit code, after printing
/// the result to stdout or the error to stderr.
pub async fn main(args: &[String]) -> i32 {
    let result = match args {
        [cmd, dir] if cmd == "check" => check(Path::new(dir)).await.map(|n| format!("{n} vectors match")),
        [cmd, out, sources @ ..] if cmd == "write" && !sources.is_empty() => {
            let sources: Vec<PathBuf> = sources.iter().map(PathBuf::from).collect();
            write(Path::new(out), &sources).await.map(|n| format!("{n} vectors written"))
        }
        _ => {
            eprintln!("usage: tparquet-datafusion-vectors check DIR | write OUT SRC...");
            return 2;
        }
    };
    match result {
        Ok(line) => {
            println!("{line}");
            0
        }
        Err(e) => {
            eprintln!("tparquet-datafusion-vectors: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_difference_counts_missing_lines() {
        assert_eq!(first_difference("", ""), None);
        assert_eq!(first_difference("a\nb\n", "a\nb\n"), None);
        assert_eq!(first_difference("a\nb\n", "a\nc\n"), Some(1));
        assert_eq!(first_difference("a\n", "a\nb\n"), Some(1));
        assert_eq!(first_difference("a\nb\n", "a\n"), Some(1));
        assert_eq!(first_difference("x\n", ""), Some(0));
    }

    #[test]
    fn literals_quote_quotes() {
        assert_eq!(literal("a'b"), "'a''b'");
        assert_eq!(literal("/tmp/x.parquet"), "'/tmp/x.parquet'");
    }

    #[tokio::test]
    async fn main_reports_through_its_exit_code() {
        assert_eq!(main(&[]).await, 2);
        assert_eq!(main(&["check".into(), "/nonexistent/vectors".into()]).await, 1);
        let testdata = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata");
        assert_eq!(main(&["check".into(), testdata.into()]).await, 0);
    }
}
