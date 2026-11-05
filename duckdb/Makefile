# The DuckDB extension `tparquet`. Run from duckdb/ (or `make -C duckdb ...`).
#
#   make build     release build: build/tparquet.duckdb_extension (and the tparquet CLI)
#   make test      unit tests, then the SQL tests on the extension cargo builds for them
#   make sql       the SQL tests on build/tparquet.duckdb_extension
#   make mutation  cargo mutants on the extension crate (SQL tests without the `slow` ones)
#   make check     fmt, clippy, one arrow version, test
CARGO ?= cargo
UV ?= uv
JOBS ?= 2
PYTHON := .venv/bin/python
ROOT := ..
# cargo builds into $CARGO_TARGET_DIR when it is set
TARGET := $(or $(CARGO_TARGET_DIR),$(ROOT)/target)
CRATE := -p tparquet-duckdb
DYLIB := $(if $(filter Darwin,$(shell uname)),libtparquet_duckdb.dylib,libtparquet_duckdb.so)
EXTENSION := build/tparquet.duckdb_extension
MUTANTS_OUT ?= $(TARGET)/mutants-duckdb
MEMORY_LIMIT_MB ?= 2048
TEST_TIMEOUT ?= 60

.PHONY: venv build test sql mutation check fmt-check clippy check-arrow clean

venv: .venv/.synced

# a stamp file of our own: .venv/bin/python is a symlink to the interpreter uv found, which
# `touch` would follow (on CI runners that interpreter is not writable)
.venv/.synced: pyproject.toml uv.lock
	$(UV) sync --python 3.14 --frozen
	@touch $@

build:
	$(CARGO) build --release -j $(JOBS) $(CRATE) -p tparquet --lib --bin tparquet --bin package-extension
	@mkdir -p build
	rm -f $(EXTENSION)
	$(TARGET)/release/package-extension $(TARGET)/release/$(DYLIB) $(EXTENSION)
	@echo "built $(EXTENSION)"

test: venv
	$(CARGO) test -j $(JOBS) $(CRATE)

sql: venv build
	TPARQUET_DUCKDB_EXTENSION=$(abspath $(EXTENSION)) TPARQUET_CLI=$(abspath $(TARGET)/release/tparquet) \
		$(PYTHON) -m pytest -q tests

# Every viable mutant must be caught. The slow memory test is left out of the mutation run:
# a mutant that only it could catch would also have to keep every other result right.
mutation: venv
	@# cargo mutants creates the output directory but not its parents (a fresh clone has no target/)
	mkdir -p $(MUTANTS_OUT)
	@# exit 3 means some mutants made the tests hang, which detects them; it is accepted only
	@# when none was missed (the root Makefile's MUTANTS_OK)
	@# MEMORY_LIMIT_MB: a mutant that makes a scan endless grows ~65 MB/s and would exhaust the
	@# runner before cargo mutants' timeout; tests/conftest.py ends a test process past it
	@# (the suite needs under 1 GB; JOBS processes run at once). TEST_TIMEOUT ends a test process
	@# when one test takes longer (the slowest takes a few seconds), before cargo mutants' timeout
	TPARQUET_DUCKDB_MEMORY_LIMIT_MB=$(MEMORY_LIMIT_MB) TPARQUET_DUCKDB_TEST_TIMEOUT=$(TEST_TIMEOUT) \
	TPARQUET_DUCKDB_PYTEST_MARK="not slow" TPARQUET_DUCKDB_PYTHON=$(abspath $(PYTHON)) \
		$(CARGO) mutants --dir $(ROOT) $(CRATE) -j $(JOBS) --output $(MUTANTS_OUT) $(MUTANTS_ARGS) \
		|| { [ $$? -eq 3 ] && [ ! -s $(MUTANTS_OUT)/mutants.out/missed.txt ]; }

fmt-check:
	$(CARGO) fmt $(CRATE) --check

clippy:
	$(CARGO) clippy -j $(JOBS) --no-deps $(CRATE) --all-targets -- -D warnings

# the duckdb crate depends on arrow ^60: it must share arrow 60.1.0 with the parquet fork
check-arrow:
	@dups="$$($(CARGO) tree $(CRATE) -e normal --duplicates --depth 0 --prefix none \
		| awk '/^(arrow|parquet)/ {print $$1}' | sort -u)"; \
	if [ -n "$$dups" ]; then echo "more than one version of:" $$dups; exit 1; fi; \
	$(CARGO) tree $(CRATE) -e normal --prefix none -f '{p}' | grep -E '^(arrow |arrow-array |parquet )' | sort -u

check: fmt-check clippy check-arrow test

clean:
	rm -f $(EXTENSION)
