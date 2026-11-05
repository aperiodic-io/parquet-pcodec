#!/usr/bin/env bash
# Checks the parquet-java fork in java/parquet-java against upstream (java/parquet-java/FORK.md):
# downloads the published -sources.jar of each forked module, verifies its sha256, unpacks it
# the way the fork was vendored (.java files under src/main/java, the other files under
# src/main/resources, without the jar's build metadata META-INF/MANIFEST.MF and
# META-INF/maven/**), and fails unless the diff between that and the vendored tree (git-tracked
# files, without FORK.md and FORK.patch) is exactly java/parquet-java/FORK.patch.
#
#   scripts/check-fork-java.sh                 check
#   scripts/check-fork-java.sh --update        rewrite java/parquet-java/FORK.patch from the tree
#   scripts/check-fork-java.sh --upstream DIR  only write the upstream tree to DIR
#
# MAVEN_CENTRAL overrides the repository URL (a mirror of Maven Central); the hashes still
# have to match.
set -euo pipefail

version=1.17.0
modules=(parquet-format-structures parquet-common parquet-hadoop)
declare -A sha256=(
  [parquet-format-structures]=bff6057ba3517a3310b11acc95cba2a12b5e0379e89e338751009d8df134d2de
  [parquet-common]=ed385971f09275d4919f17f9c77256c86e47d0707359e2cdde939a2401a91dcc
  [parquet-hadoop]=86d3dade33a169701a2d25d4739636cea0fc858abae8027d803217a8763045e8
)
central=${MAVEN_CENTRAL:-https://repo.maven.apache.org/maven2}

root=$(cd "$(dirname "$0")/.." && pwd)
fork="$root/java/parquet-java"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# upstream <dir>: the upstream tree of every module under <dir>/<module>
upstream() {
  local out=$1 m jar
  for m in "${modules[@]}"; do
    grep -q "${sha256[$m]}" "$fork/FORK.md" || { echo "check-fork-java: FORK.md does not record sha256 ${sha256[$m]} ($m)" >&2; exit 1; }
    jar="$work/$m-$version-sources.jar"
    curl -sSfL --retry 5 --retry-all-errors -o "$jar" "$central/org/apache/parquet/$m/$version/$m-$version-sources.jar"
    echo "${sha256[$m]}  $jar" | sha256sum --check --quiet -
    mkdir -p "$work/jar-$m" "$out/$m/src/main/java" "$out/$m/src/main/resources"
    unzip -q "$jar" -d "$work/jar-$m" -x 'META-INF/MANIFEST.MF' 'META-INF/maven/*'
    (cd "$work/jar-$m" &&
      find . -type f -name '*.java' -exec cp --parents -t "$out/$m/src/main/java" {} + &&
      find . -type f ! -name '*.java' -exec cp --parents -t "$out/$m/src/main/resources" {} +)
    rmdir --ignore-fail-on-non-empty "$out/$m/src/main/resources"
  done
}

if [ "${1:-}" = "--upstream" ]; then
  upstream "$2"
  exit 0
fi

upstream "$work/upstream"
mkdir "$work/fork"
(cd "$fork" && git ls-files -z -- . ':!FORK.md' ':!FORK.patch' | xargs -0 cp --parents -t "$work/fork")

cd "$work"
status=0
git -c core.quotepath=off diff --no-index --no-color --no-ext-diff --no-renames --full-index \
    --diff-algorithm=myers --no-prefix upstream fork > fork.diff || status=$?
if [ "$status" -gt 1 ]; then
    echo "check-fork-java: git diff failed" >&2
    exit "$status"
fi

if [ "${1:-}" = "--update" ]; then
    cp fork.diff "$fork/FORK.patch"
    echo "check-fork-java: wrote java/parquet-java/FORK.patch ($(grep -c '^diff --git' fork.diff) files, $(wc -l < fork.diff) lines)"
    exit 0
fi

if ! cmp -s fork.diff "$fork/FORK.patch"; then
    echo "check-fork-java: java/parquet-java differs from upstream parquet-java ${version} + FORK.patch:" >&2
    diff -u "$fork/FORK.patch" fork.diff >&2 || true
    echo "check-fork-java: if the change is intended, run scripts/check-fork-java.sh --update and list it in FORK.md" >&2
    exit 1
fi
echo "check-fork-java: java/parquet-java is upstream parquet-java ${version} (${modules[*]}) + FORK.patch ($(grep -c '^diff --git' fork.diff) files)"
