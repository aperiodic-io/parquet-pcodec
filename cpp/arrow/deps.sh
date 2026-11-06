#!/usr/bin/env bash
# Builds the two Arrow 26 dependencies that Ubuntu 24.04 packages too old (Arrow needs xsimd
# >= 14.2.0 and simdjson >= 4.3.0; noble has 12.1.1 and 3.6.4) from Debian's source tarballs,
# verified by sha256, into target/cpp/deps. Everything else comes from apt (cpp/README.md).
# Arrow's own bundled downloads go to github.com, which is not reachable from every build
# machine; deb.debian.org and snapshot.debian.org (permanent, by sha1) are.
#
#   cpp/arrow/deps.sh            prints the install prefix on its last line
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
dl="$root/target/cpp/dl"
prefix="$root/target/cpp/deps"
jobs=${JOBS:-3}

# name version debian-pool-dir sha256 sha1
deps=(
    "xsimd 14.3.0 x/xsimd 110a4609e47d1f078170463bdf78401588b8e5148ba18752fcfd5af627f49ae2 7e3105391ca3915c23e8a7dc24339aaae748481e"
    "simdjson 4.6.11 s/simdjson aefa2d971fa203c72160a762b40f169697036cbf50d9ad8c4cc022a44c7294c0 969c0df6b5dd82c4f4786c3b8c8ec5dba80778e7"
)

stamp="$prefix/.stamp"
want=$(printf '%s\n' "${deps[@]}" | sha256sum | cut -d' ' -f1)
if [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$want" ]; then
    echo "$prefix"
    exit 0
fi

mkdir -p "$dl"
rm -rf "$prefix" "$root/target/cpp/deps-build"
for dep in "${deps[@]}"; do
    read -r name version pool sha256 sha1 <<< "$dep"
    file="${name}_${version}.orig.tar.gz"
    ok() { echo "$sha256  $dl/$file" | sha256sum --check --quiet --status - 2>/dev/null; }
    if ! ok; then
        curl -sSfL --retry 3 -o "$dl/$file" "https://deb.debian.org/debian/pool/main/$pool/$file" ||
            curl -sSfL --retry 3 -o "$dl/$file" "https://snapshot.debian.org/file/$sha1"
        ok || { echo "deps: sha256 mismatch for $file" >&2; rm -f "$dl/$file"; exit 1; }
    fi
    src="$root/target/cpp/deps-build/$name-src"
    mkdir -p "$src"
    tar -xzf "$dl/$file" -C "$src" --strip-components=1
    cmake -S "$src" -B "$src/build" -G Ninja -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$prefix" -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_TESTING=OFF \
        -DSIMDJSON_DEVELOPER_MODE=OFF -DSIMDJSON_ENABLE_THREADS=ON > /dev/null
    cmake --build "$src/build" -j "$jobs" > /dev/null
    cmake --install "$src/build" > /dev/null
done
rm -rf "$root/target/cpp/deps-build"
echo "$want" > "$stamp"
echo "$prefix"
