#!/usr/bin/env bash
# Cross-language acceptance: every implementation reads the vectors every other one wrote.
#
#   writers: rust (testdata/), go (pcodec-go-gen), python (transcode), duckdb (COPY)
#   readers: rust (gen-testdata --check), go (pcodec-go-gen -check), python, duckdb
#
# Needs: the Rust workspace, Go, python/.venv with the package installed (`make python-test`
# or `maturin develop`), and the built DuckDB extension (`make -C duckdb build`).
# Usage: acceptance/cross/run.sh [OUT]   (default: target/cross)
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
out=${1:-$root/target/cross}
py=${PYTHON:-$root/python/.venv/bin/python}
ext=${PCODEC_DUCKDB_EXTENSION:-$root/duckdb/build/parquet_pcodec.duckdb_extension}
vectors="$root/acceptance/cross/vectors.py"
mkdir -p "$out"

cargo build -q --manifest-path "$root/Cargo.toml" -p parquet-pcodec --bin gen-testdata
# the Go package links target/release/libparquet_pcodec.a (go/link.go)
cargo build -q --release --manifest-path "$root/Cargo.toml" -p parquet-pcodec-capi
rust_check() { "$root/target/debug/gen-testdata" --check "$1"; }
(cd "$root/go" && go build -o "$out/pcodec-go-gen" ./cmd/pcodec-go-gen)
go_check() { "$out/pcodec-go-gen" -check "$1"; }
python_check() { "$py" "$vectors" check-python "$1"; }
duckdb_check() { "$py" "$vectors" check-duckdb "$ext" "$1"; }

echo "== writers"
"$out/pcodec-go-gen" "$out/go" > /dev/null
"$py" "$vectors" write-python "$out/python" "$root/testdata" "$out/go"
"$py" "$vectors" write-duckdb "$ext" "$out/duckdb" "$root/testdata" "$out/go"

declare -A dirs=([rust]="$root/testdata" [go]="$out/go" [python]="$out/python" [duckdb]="$out/duckdb")
for writer in rust go python duckdb; do
  for reader in rust go python duckdb; do
    printf '%-7s -> %-7s ' "$writer" "$reader"
    "${reader}_check" "${dirs[$writer]}"
  done
done

# every checker must also notice a difference: one changed row in a copy of the Rust vectors
echo "== the checkers fail on a changed row"
bad="$out/tampered"
rm -rf "$bad" && mkdir -p "$bad"
cp -r "$root/testdata/files" "$root/testdata/expected" "$bad/"
sed -i '2s/1/2/' "$bad/expected/temporal.jsonl"
for reader in rust go python duckdb; do
  if "${reader}_check" "$bad" > /dev/null 2>&1; then
    echo "$reader accepted a changed row" >&2
    exit 1
  fi
  echo "$reader: refused"
done
echo "cross-language acceptance: ok"
