#!/usr/bin/env bash
# Checks the JVM forks against upstream (each fork's FORK.md):
#
#   java/parquet-java       parquet-java 1.17.0: parquet-format-structures, parquet-common, parquet-hadoop
#   java/parquet-java-1.18  parquet-java 1.18.1: parquet-format-structures, parquet-common (for Trino)
#   trino/trino-parquet     trino-parquet 484 (Trino's Parquet reader and writer)
#
# For each fork it downloads the published -sources.jar of each forked module, verifies its
# sha256, unpacks it the way the fork was vendored (.java files under src/main/java, the other
# files under src/main/resources, without the jar's build metadata META-INF/MANIFEST.MF and
# META-INF/maven/**), and fails unless the diff between that and the vendored tree (git-tracked
# files, without FORK.md and FORK.patch) is exactly the fork's FORK.patch.
#
#   scripts/check-fork-java.sh [FORK...]                 check (default: every fork)
#   scripts/check-fork-java.sh --update [FORK...]        rewrite FORK.patch from the tree
#   scripts/check-fork-java.sh --upstream DIR [FORK...]  only write the upstream trees: with one
#                                                        FORK, DIR is that fork's tree (DIR/<module>
#                                                        for parquet-java); else DIR/<FORK>
#
# MAVEN_CENTRAL overrides the repository URL (a mirror of Maven Central); the hashes still
# have to match.
set -euo pipefail

all_forks=(java/parquet-java java/parquet-java-1.18 trino/trino-parquet)
# per fork: Maven group path, version, and the modules as artifact=directory ('.' is the fork's root)
declare -A group=(
  [java/parquet-java]=org/apache/parquet
  [java/parquet-java-1.18]=org/apache/parquet
  [trino/trino-parquet]=io/trino
)
declare -A version=(
  [java/parquet-java]=1.17.0
  [java/parquet-java-1.18]=1.18.1
  [trino/trino-parquet]=484
)
declare -A modules=(
  [java/parquet-java]="parquet-format-structures=parquet-format-structures parquet-common=parquet-common parquet-hadoop=parquet-hadoop"
  [java/parquet-java-1.18]="parquet-format-structures=parquet-format-structures parquet-common=parquet-common"
  [trino/trino-parquet]="trino-parquet=."
)
declare -A sha256=(
  [java/parquet-java/parquet-format-structures]=bff6057ba3517a3310b11acc95cba2a12b5e0379e89e338751009d8df134d2de
  [java/parquet-java/parquet-common]=ed385971f09275d4919f17f9c77256c86e47d0707359e2cdde939a2401a91dcc
  [java/parquet-java/parquet-hadoop]=86d3dade33a169701a2d25d4739636cea0fc858abae8027d803217a8763045e8
  [java/parquet-java-1.18/parquet-format-structures]=e3fa98021ee040452c2ba9a4715b5e5a9e36ca6447906b590ca114345f2df99f
  [java/parquet-java-1.18/parquet-common]=25fc79cbb1414f81537cdfae1353623f9a972ca4dffa3c3837ac5ff9c5a69f35
  [trino/trino-parquet/trino-parquet]=9bf84b7f04d461c03dc757f02094cfd68b835cd89dacc31b3fcca8c2cd272824
)
central=${MAVEN_CENTRAL:-https://repo.maven.apache.org/maven2}
central=${central%/}

root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

mode=check
out=
case "${1:-}" in
  --update) mode=update; shift ;;
  --upstream)
    mode=upstream; out=${2:?--upstream needs a directory}; shift 2
    mkdir -p "$out"; out=$(cd "$out" && pwd) ;;
esac
forks=("$@")
[ ${#forks[@]} -gt 0 ] || forks=("${all_forks[@]}")
for f in "${forks[@]}"; do
  [ -n "${version[$f]:-}" ] || { echo "check-fork-java: unknown fork $f (one of: ${all_forks[*]})" >&2; exit 2; }
done

# upstream <fork> <dir>: the fork's upstream tree under <dir> (<dir>/<module directory>/src/...)
upstream() {
  local fork=$1 dest=$2 entry artifact sub jar sum v=${version[$1]} tree
  for entry in ${modules[$fork]}; do
    artifact=${entry%%=*} sub=${entry#*=}
    sum=${sha256[$fork/$artifact]}
    grep -q "$sum" "$root/$fork/FORK.md" || { echo "check-fork-java: $fork/FORK.md does not record sha256 $sum ($artifact)" >&2; exit 1; }
    jar="$work/$artifact-$v-sources.jar"
    curl -sSfL --retry 5 --retry-all-errors -o "$jar" "$central/${group[$fork]}/$artifact/$v/$artifact-$v-sources.jar"
    echo "$sum  $jar" | sha256sum --check --quiet -
    tree="$dest/$sub"
    rm -rf "$work/jar"
    mkdir -p "$work/jar" "$tree/src/main/java" "$tree/src/main/resources"
    unzip -q "$jar" -d "$work/jar" -x 'META-INF/MANIFEST.MF' 'META-INF/maven/*'
    (cd "$work/jar" &&
      find . -type f -name '*.java' -exec cp --parents -t "$tree/src/main/java" {} + &&
      { find . -type f ! -name '*.java' -print0 | xargs -0 -r cp --parents -t "$tree/src/main/resources"; })
    rmdir --ignore-fail-on-non-empty "$tree/src/main/resources"
  done
}

if [ "$mode" = upstream ]; then
  if [ ${#forks[@]} -eq 1 ]; then
    upstream "${forks[0]}" "$out"
  else
    for f in "${forks[@]}"; do upstream "$f" "$out/$f"; done
  fi
  exit 0
fi

failed=0
for fork in "${forks[@]}"; do
  w="$work/${fork//\//_}"
  mkdir -p "$w/fork"
  upstream "$fork" "$w/upstream"
  (cd "$root/$fork" && git ls-files -z -- . ':!FORK.md' ':!FORK.patch' | xargs -0 -r cp --parents -t "$w/fork")
  status=0
  (cd "$w" && git -c core.quotepath=off diff --no-index --no-color --no-ext-diff --no-renames --full-index \
      --diff-algorithm=myers --no-prefix upstream fork > fork.diff) || status=$?
  if [ "$status" -gt 1 ]; then
    echo "check-fork-java: git diff failed ($fork)" >&2
    exit "$status"
  fi
  files=$(grep -c '^diff --git' "$w/fork.diff" || true)
  if [ "$mode" = update ]; then
    cp "$w/fork.diff" "$root/$fork/FORK.patch"
    echo "check-fork-java: wrote $fork/FORK.patch ($files files, $(wc -l < "$w/fork.diff") lines)"
  elif ! cmp -s "$w/fork.diff" "$root/$fork/FORK.patch"; then
    echo "check-fork-java: $fork differs from upstream ${version[$fork]} + FORK.patch:" >&2
    diff -u "$root/$fork/FORK.patch" "$w/fork.diff" >&2 || true
    echo "check-fork-java: if the change is intended, run scripts/check-fork-java.sh --update $fork and list it in $fork/FORK.md" >&2
    failed=1
  else
    echo "check-fork-java: $fork is upstream ${version[$fork]} ($(for e in ${modules[$fork]}; do printf '%s ' "${e%%=*}"; done | sed 's/ $//')) + FORK.patch ($files files)"
  fi
done
exit "$failed"
