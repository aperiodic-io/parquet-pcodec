#!/usr/bin/env bash
# Checks the forks of the arrow-rs parquet crate against upstream (each fork's FORK.md):
# downloads the upstream crate, verifies its sha256, and fails unless the diff between it and
# the vendored tree (git-tracked files, without FORK.md and FORK.patch) is exactly the fork's
# FORK.patch.
#
#   scripts/check-fork.sh [DIR...]            check every fork (or only those in DIR...)
#   scripts/check-fork.sh --update [DIR...]   rewrite their FORK.patch from the tree
set -euo pipefail

# the forks: directory, upstream parquet version, sha256 of the published .crate
forks=(
    "rust/parquet 60.1.0 806eaa0f1c0436dcb43bee8e6a82bf2efff90521a60172f8281cb9ef577b43a4"
    "datafusion/parquet 59.3.2 8eb260d566f2fbdabe356d26aaa5d13403aac29b5b4e44463b8f9bd7b2b12680"
)

update=false
if [ "${1:-}" = "--update" ]; then
    update=true
    shift
fi

root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

selected=()
for dir in "$@"; do
    dir=${dir%/}
    found=false
    for entry in "${forks[@]}"; do
        if [ "${entry%% *}" = "$dir" ]; then
            selected+=("$entry")
            found=true
        fi
    done
    if [ "$found" = false ]; then
        echo "check-fork: $dir is not a fork (known: ${forks[*]%% *})" >&2
        exit 2
    fi
done
if [ "${#selected[@]}" -eq 0 ]; then
    selected=("${forks[@]}")
fi

# check_one DIR VERSION SHA256: returns 0 when the fork matches (or was updated), 1 otherwise.
# errexit does not apply inside a function called with ||, so every step checks itself.
check_one() {
    local dir=$1 version=$2 sha256=$3
    local fork="$root/$dir"
    local url="https://static.crates.io/crates/parquet/parquet-${version}.crate"
    local w="$work/${dir//\//_}"
    mkdir "$w" "$w/unpacked" "$w/fork" || return 1

    grep -q "$sha256" "$fork/FORK.md" || { echo "check-fork: $dir/FORK.md does not record sha256 $sha256" >&2; return 1; }

    curl -sSfL --retry 3 -o "$w/parquet.crate" "$url" || return 1
    echo "$sha256  $w/parquet.crate" | sha256sum --check --quiet - || return 1
    tar -xzf "$w/parquet.crate" -C "$w/unpacked" || return 1
    mv "$w/unpacked/parquet-${version}" "$w/upstream" || return 1
    rm "$w/upstream/Cargo.toml.orig" || return 1
    (cd "$fork" && git ls-files -z -- . ':!FORK.md' ':!FORK.patch' | xargs -0 cp --parents -t "$w/fork") || return 1

    local status=0
    (cd "$w" && git -c core.quotepath=off diff --no-index --no-color --no-ext-diff --no-renames --full-index \
        --diff-algorithm=myers --no-prefix upstream fork > fork.diff) || status=$?
    if [ "$status" -gt 1 ]; then
        echo "check-fork: git diff failed" >&2
        return 1
    fi
    local files
    files=$(grep -c '^diff --git' "$w/fork.diff" || true)

    if [ "$update" = true ]; then
        cp "$w/fork.diff" "$fork/FORK.patch" || return 1
        echo "check-fork: wrote $dir/FORK.patch ($files files, $(wc -l < "$w/fork.diff") lines)"
        return 0
    fi

    if ! cmp -s "$w/fork.diff" "$fork/FORK.patch"; then
        echo "check-fork: $dir differs from upstream parquet ${version} + FORK.patch:" >&2
        diff -u "$fork/FORK.patch" "$w/fork.diff" >&2 || true
        echo "check-fork: if the change is intended, run scripts/check-fork.sh --update and list it in FORK.md" >&2
        return 1
    fi
    echo "check-fork: $dir is upstream parquet ${version} (sha256 ${sha256}) + FORK.patch ($files files)"
}

failed=0
for entry in "${selected[@]}"; do
    read -r dir version sha256 <<< "$entry"
    check_one "$dir" "$version" "$sha256" || failed=1
done
exit "$failed"
