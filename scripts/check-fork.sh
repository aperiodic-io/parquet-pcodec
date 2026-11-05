#!/usr/bin/env bash
# Checks the parquet fork in rust/parquet against upstream (rust/parquet/FORK.md):
# downloads the upstream crate, verifies its sha256, and fails unless the diff between it and
# the vendored tree (git-tracked files, without FORK.md and FORK.patch) is exactly
# rust/parquet/FORK.patch.
#
#   scripts/check-fork.sh            check
#   scripts/check-fork.sh --update   rewrite rust/parquet/FORK.patch from the tree
set -euo pipefail

version=60.1.0
sha256=806eaa0f1c0436dcb43bee8e6a82bf2efff90521a60172f8281cb9ef577b43a4
url="https://static.crates.io/crates/parquet/parquet-${version}.crate"

root=$(cd "$(dirname "$0")/.." && pwd)
fork="$root/rust/parquet"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

grep -q "$sha256" "$fork/FORK.md" || { echo "check-fork: FORK.md does not record sha256 $sha256" >&2; exit 1; }

curl -sSfL --retry 3 -o "$work/parquet.crate" "$url"
echo "$sha256  $work/parquet.crate" | sha256sum --check --quiet -
mkdir "$work/unpacked"
tar -xzf "$work/parquet.crate" -C "$work/unpacked"
mv "$work/unpacked/parquet-${version}" "$work/upstream"
rm "$work/upstream/Cargo.toml.orig"

mkdir "$work/fork"
(cd "$fork" && git ls-files -z -- . ':!FORK.md' ':!FORK.patch' | xargs -0 cp --parents -t "$work/fork")

cd "$work"
status=0
git -c core.quotepath=off diff --no-index --no-color --no-ext-diff --no-renames --full-index \
    --diff-algorithm=myers --no-prefix upstream fork > fork.diff || status=$?
if [ "$status" -gt 1 ]; then
    echo "check-fork: git diff failed" >&2
    exit "$status"
fi

if [ "${1:-}" = "--update" ]; then
    cp fork.diff "$fork/FORK.patch"
    echo "check-fork: wrote rust/parquet/FORK.patch ($(grep -c '^diff --git' fork.diff) files, $(wc -l < fork.diff) lines)"
    exit 0
fi

if ! cmp -s fork.diff "$fork/FORK.patch"; then
    echo "check-fork: rust/parquet differs from upstream parquet ${version} + FORK.patch:" >&2
    diff -u "$fork/FORK.patch" fork.diff >&2 || true
    echo "check-fork: if the change is intended, run scripts/check-fork.sh --update and list it in FORK.md" >&2
    exit 1
fi
echo "check-fork: rust/parquet is upstream parquet ${version} (sha256 ${sha256}) + FORK.patch ($(grep -c '^diff --git' fork.diff) files)"
