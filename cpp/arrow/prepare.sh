#!/usr/bin/env bash
# Prepares the patched Apache Arrow C++ source tree (cpp/arrow/FORK.md): downloads the Arrow
# source release (cached in target/cpp/dl), verifies its sha512, extracts it and applies
# cpp/arrow/FORK.patch.
#
#   cpp/arrow/prepare.sh [DEST]     default DEST: target/cpp/arrow-src
#
# DEST is left alone when it already holds this FORK.patch (DEST/.tparquet-fork records the
# patch's sha256). To change the fork, edit DEST and run scripts/check-cpp-fork.sh --update.
set -euo pipefail

version=26.0.1
sha512=2ee7efc6ae5b1a2d2c4f2daa52110d16d2e64c02f27f341b462459341528bdcdc796c043c4834baa80dbcb4bac0219d1c705741485973e9207656124f83e74dd
url="https://dlcdn.apache.org/arrow/arrow-${version}/apache-arrow-${version}.tar.gz"

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
dest=${1:-$root/target/cpp/arrow-src}
dl="$root/target/cpp/dl"
tarball="$dl/apache-arrow-${version}.tar.gz"

grep -q "$sha512" "$here/FORK.md" || { echo "prepare: FORK.md does not record sha512 $sha512" >&2; exit 1; }

patch_sum=$(sha256sum "$here/FORK.patch" | cut -d' ' -f1)
if [ -f "$dest/.tparquet-fork" ] && [ "$(cat "$dest/.tparquet-fork")" = "$patch_sum" ]; then
    echo "prepare: $dest is Arrow ${version} + FORK.patch"
    exit 0
fi

verified() { echo "$sha512  $tarball" | sha512sum --check --quiet --status - 2>/dev/null; }
if ! verified; then
    mkdir -p "$dl"
    curl -sSfL --retry 3 -o "$tarball.part" "$url"
    mv "$tarball.part" "$tarball"
    verified || { echo "prepare: sha512 mismatch for $url" >&2; rm -f "$tarball"; exit 1; }
fi

rm -rf "$dest" "$dest.tmp"
mkdir -p "$dest.tmp"
tar -xzf "$tarball" -C "$dest.tmp" --strip-components=1
(cd "$dest.tmp" && patch -p1 --batch --forward --quiet < "$here/FORK.patch")
echo "$patch_sum" > "$dest.tmp/.tparquet-fork"
mv "$dest.tmp" "$dest"
echo "prepare: $dest is Arrow ${version} (sha512 ${sha512:0:16}...) + FORK.patch"
