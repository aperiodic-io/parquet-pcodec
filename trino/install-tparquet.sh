#!/usr/bin/env bash
# Makes a Trino 484 install read tparquet files (trino/README.md): in every plugin directory that
# holds trino-parquet (hive, iceberg, delta-lake, hudi, ...), replaces
#
#   trino-parquet-484.jar                 by trino-parquet-484-tparquet.jar
#   parquet-format-structures-1.18.1.jar  by parquet-format-structures-1.18.1-tparquet.jar
#   parquet-common-1.18.1.jar             by parquet-common-1.18.1-tparquet.jar
#
# and adds tparquet-jni-<version>.jar (the codec, with its native library). The upstream jars are
# moved to TRINO_HOME/tparquet-backup/<plugin>/, so that --uninstall can put them back. Every
# plugin directory is checked before anything is changed; a directory of another Trino or
# parquet-java version is an error. Running it again updates the installed jars.
#
#   trino/install-tparquet.sh [--jars DIR] TRINO_HOME
#   trino/install-tparquet.sh --uninstall TRINO_HOME
#
# DIR holds the four jars (default: trino/plugin-jars/target/tparquet-jars, from `make -C trino
# plugin-jars`). Restart Trino afterwards.
set -euo pipefail

trino_version=484
parquet_version=1.18.1
fork=tparquet

usage() {
  echo "usage: $0 [--jars DIR] TRINO_HOME | $0 --uninstall TRINO_HOME" >&2
  exit 2
}
fail() {
  echo "install-tparquet: $*" >&2
  exit 1
}

jars="$(cd "$(dirname "$0")" && pwd)/plugin-jars/target/tparquet-jars"
uninstall=0
while [ $# -gt 1 ]; do
  case "$1" in
    --jars) [ $# -ge 3 ] || usage; jars=$2; shift 2 ;;
    --uninstall) uninstall=1; shift ;;
    *) usage ;;
  esac
done
[ $# -eq 1 ] || usage
case "$1" in -*) usage ;; esac
home=${1%/}
[ -d "$home/plugin" ] || fail "$home is not a Trino install (no plugin directory)"
backup="$home/tparquet-backup"

# the one file of a plugin directory matching a pattern, or nothing (more than one is an error)
one() {
  local dir=$1 pattern=$2 found=()
  shopt -s nullglob
  found=("$dir"/$pattern)
  shopt -u nullglob
  [ ${#found[@]} -le 1 ] || fail "$dir has ${#found[@]} jars matching $pattern: ${found[*]##*/}"
  [ ${#found[@]} -eq 0 ] || echo "${found[0]}"
}

# the plugin directories that use trino-parquet
plugins=()
for dir in "$home"/plugin/*/; do
  dir=${dir%/}
  [ -d "$dir" ] || continue
  jar=$(one "$dir" 'trino-parquet-*.jar')
  if [ -n "$jar" ]; then
    plugins+=("$dir")
  fi
done
[ ${#plugins[@]} -gt 0 ] || fail "no plugin directory of $home holds trino-parquet"

if [ "$uninstall" = 1 ]; then
  for dir in "${plugins[@]}"; do
    name=${dir##*/}
    [ -d "$backup/$name" ] || { echo "install-tparquet: $name: not installed"; continue; }
    for f in "$backup/$name"/*.jar; do
      [ -e "$f" ] || fail "$backup/$name has no jars"
    done
  done
  for dir in "${plugins[@]}"; do
    name=${dir##*/}
    [ -d "$backup/$name" ] || continue
    rm -f "$dir/trino-parquet-$trino_version-$fork.jar" "$dir/parquet-format-structures-$parquet_version-$fork.jar" \
      "$dir/parquet-common-$parquet_version-$fork.jar" "$dir"/tparquet-jni-*.jar
    mv "$backup/$name"/*.jar "$dir/"
    rmdir "$backup/$name"
    echo "install-tparquet: $name: upstream jars restored"
  done
  rmdir "$backup" 2>/dev/null || true
  exit 0
fi

# the jars to install
[ -d "$jars" ] || fail "no jar directory $jars (make -C trino plugin-jars)"
new_trino_parquet="$jars/trino-parquet-$trino_version-$fork.jar"
new_format="$jars/parquet-format-structures-$parquet_version-$fork.jar"
new_common="$jars/parquet-common-$parquet_version-$fork.jar"
new_jni=$(one "$jars" 'tparquet-jni-*.jar')
for f in "$new_trino_parquet" "$new_format" "$new_common" "$new_jni"; do
  [ -n "$f" ] && [ -f "$f" ] || fail "$jars must hold trino-parquet-$trino_version-$fork.jar, parquet-format-structures-$parquet_version-$fork.jar, parquet-common-$parquet_version-$fork.jar and tparquet-jni-*.jar"
done

# check every plugin directory first
for dir in "${plugins[@]}"; do
  name=${dir##*/}
  for artifact in trino-parquet parquet-format-structures parquet-common; do
    if [ "$artifact" = trino-parquet ]; then v=$trino_version; else v=$parquet_version; fi
    jar=$(one "$dir" "$artifact-[0-9]*.jar")
    case "${jar##*/}" in
      "$artifact-$v.jar") ;;
      "$artifact-$v-$fork.jar") [ -f "$backup/$name/$artifact-$v.jar" ] || fail "$name: $artifact-$v-$fork.jar without a backup of $artifact-$v.jar in $backup/$name" ;;
      "") fail "$name: no $artifact jar (expected $artifact-$v.jar)" ;;
      *) fail "$name: ${jar##*/} is not $artifact $v (this is for Trino $trino_version)" ;;
    esac
  done
done

for dir in "${plugins[@]}"; do
  name=${dir##*/}
  mkdir -p "$backup/$name"
  for artifact in trino-parquet parquet-format-structures parquet-common; do
    if [ "$artifact" = trino-parquet ]; then v=$trino_version; else v=$parquet_version; fi
    if [ -f "$dir/$artifact-$v.jar" ]; then
      mv "$dir/$artifact-$v.jar" "$backup/$name/"
    fi
  done
  rm -f "$dir"/tparquet-jni-*.jar
  cp "$new_trino_parquet" "$new_format" "$new_common" "$new_jni" "$dir/"
  echo "install-tparquet: $name: trino-parquet, parquet-format-structures and parquet-common replaced, ${new_jni##*/} added"
done
