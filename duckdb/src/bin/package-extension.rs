//! `package-extension LIBRARY OUTPUT [PLATFORM]`: appends DuckDB's extension metadata to the
//! built shared library, giving the loadable `pparquet.duckdb_extension`.
//!
//! PLATFORM defaults to the DuckDB name of the platform this tool runs on (`linux_amd64`...).

use pparquet_duckdb::metadata::{Metadata, current_platform, package};
use std::process::ExitCode;

fn run(args: &[String]) -> Result<(), String> {
    let (library, output, platform) = match args {
        [l, o] => (l, o, current_platform().ok_or("unknown platform: pass it as the third argument")?.to_string()),
        [l, o, p] => (l, o, p.clone()),
        _ => return Err("usage: package-extension LIBRARY OUTPUT [PLATFORM]".into()),
    };
    let bytes = std::fs::read(library).map_err(|e| format!("cannot read {library}: {e}"))?;
    let file = package(&bytes, &Metadata::for_platform(&platform))?;
    std::fs::write(output, file).map_err(|e| format!("cannot write {output}: {e}"))?;
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("package-extension: {e}");
            ExitCode::FAILURE
        }
    }
}
