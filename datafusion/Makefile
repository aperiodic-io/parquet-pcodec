# The DataFusion connector and its parquet fork (its own Cargo workspace; see README.md).
# Local equivalents of .github/workflows/datafusion.yml. `make check` runs them all but the
# slow ones (mutants, fork-mutants).
JOBS ?= 2
CARGO := cargo
OWN := -p tparquet-datafusion

.PHONY: check test fork-test vectors clippy fmt fmt-check check-fork mutants fork-mutants

check: fmt-check clippy test fork-test vectors check-fork

# the connector's unit and integration tests (tests/it), and its doc tests
test:
	$(CARGO) test -j $(JOBS) $(OWN)

# the fork's own tests (parquet/FORK.md)
fork-test:
	$(CARGO) test -j $(JOBS) -p parquet --lib -- pcodec test_write_metadata_page_indexes
	$(CARGO) test -j $(JOBS) -p parquet --test arrow_writer

# the conformance vectors through the acceptance binary
vectors:
	$(CARGO) run -q -j $(JOBS) $(OWN) --bin tparquet-datafusion-vectors -- check ../testdata

# --no-deps: the vendored fork keeps upstream's lint configuration
clippy:
	$(CARGO) clippy -j $(JOBS) --no-deps $(OWN) --all-targets -- -D warnings

# the root rustfmt.toml applies; the fork keeps upstream's formatting
fmt:
	$(CARGO) fmt $(OWN)

fmt-check:
	$(CARGO) fmt $(OWN) --check

check-fork:
	../scripts/check-fork.sh datafusion/parquet

# cargo mutants exits 3 when some mutants only made the tests hang (a timeout). A hang is a
# detected mutant, so exit 3 is accepted, but only when no mutant was missed: cargo-mutants
# also exits 3 when there are both timeouts and missed mutants, so missed.txt is checked.
MUTANTS_OK = || { [ $$? -eq 3 ] && [ ! -s mutants.out/missed.txt ]; }

# --in-place: cargo mutants copies the workspace (this directory) to a scratch tree, where the
# fork's path dependency ../../rust/core does not resolve; so the mutants are tested in this
# tree, one at a time (do not edit it meanwhile; each mutant is reverted after its test)
#
# the connector (configuration: .cargo/mutants.toml). In place, the baseline build is often a
# no-op of 0s, so the build timeout is fixed rather than a multiple of it.
BUILD_TIMEOUT ?= 1200
mutants:
	$(CARGO) mutants $(OWN) --in-place --build-timeout $(BUILD_TIMEOUT) $(MUTANTS_OK)

# the fork's changed functions only (parquet/FORK.md); the fork is rebuilt for every mutant,
# so it is built without optimization here
FORK_RE := PcodecLevel|PcodecCodec|with_pcodec_version|CodecOptionsBuilder::set_physical_type|CodecOptionsBuilder::build|create_codec|<impl From<CompressionCodec> for Compression>::from|<impl From<Compression> for CompressionCodec>::from|<impl FromStr for Compression>::from_str|write_metadata
fork-mutants:
	CARGO_PROFILE_DEV_PACKAGE_PARQUET_OPT_LEVEL=0 $(CARGO) mutants --no-config -p parquet --in-place \
	  --build-timeout $(BUILD_TIMEOUT) \
	  -f parquet/src/basic.rs -f parquet/src/compression.rs \
	  -f parquet/src/column/writer/mod.rs -f parquet/src/file/writer.rs \
	  --re '$(FORK_RE)' \
	  --cargo-arg=--lib -- -- basic:: compression:: column::writer:: file::writer:: \
	  --skip test_byte_stream_split_extended_roundtrip \
	  --skip test_rewrite_missing_column_index --skip test_rewrite_no_page_indexes \
	  $(MUTANTS_OK)
