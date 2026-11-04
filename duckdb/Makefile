# The DuckDB extension `parquet_pcodec`. Run from duckdb/ (or `make -C duckdb ...`).
#
#   make build     release build: build/parquet_pcodec.duckdb_extension (and the parquet-pcodec CLI)
#   make test      unit tests, then the SQL tests on the extension cargo builds for them
#   make sql       the SQL tests on build/parquet_pcodec.duckdb_extension
#   make mutation  cargo mutants on the extension crate (SQL tests without the `slow` ones)
#   make check     fmt, clippy, one arrow version, test
CARGO ?= cargo
UV ?= uv
JOBS ?= 2
PYTHON := .venv/bin/python
ROOT := ..
TARGET := $(ROOT)/target
CRATE := -p parquet-pcodec-duckdb
DYLIB := $(if $(filter Darwin,$(shell uname)),libparquet_pcodec_duckdb.dylib,libparquet_pcodec_duckdb.so)
EXTENSION := build/parquet_pcodec.duckdb_extension
MUTANTS_OUT ?= $(TARGET)/mutants-duckdb

.PHONY: venv build test sql mutation check fmt-check clippy check-arrow clean

venv: $(PYTHON)

$(PYTHON): pyproject.toml uv.lock
	$(UV) sync --python 3.14 --frozen
	@touch $@

build:
	$(CARGO) build --release -j $(JOBS) $(CRATE) -p parquet-pcodec --lib --bin parquet-pcodec --bin package-extension
	@mkdir -p build
	rm -f $(EXTENSION)
	$(TARGET)/release/package-extension $(TARGET)/release/$(DYLIB) $(EXTENSION)
	@echo "built $(EXTENSION)"

test: venv
	$(CARGO) test -j $(JOBS) $(CRATE)

sql: venv build
	PCODEC_DUCKDB_EXTENSION=$(abspath $(EXTENSION)) PCODEC_CLI=$(abspath $(TARGET)/release/parquet-pcodec) \
		$(PYTHON) -m pytest -q tests

# Every viable mutant must be caught. The slow memory test is left out of the mutation run:
# a mutant that only it could catch would also have to keep every other result right.
mutation: venv
	PCODEC_DUCKDB_PYTEST_MARK="not slow" PCODEC_DUCKDB_PYTHON=$(abspath $(PYTHON)) \
		$(CARGO) mutants --dir $(ROOT) $(CRATE) -j $(JOBS) --output $(MUTANTS_OUT) $(MUTANTS_ARGS)

fmt-check:
	$(CARGO) fmt $(CRATE) --check

clippy:
	$(CARGO) clippy -j $(JOBS) --no-deps $(CRATE) --all-targets -- -D warnings

# the duckdb crate depends on arrow ^60: it must share arrow 60.1.0 with the parquet fork
check-arrow:
	@dups="$$($(CARGO) tree $(CRATE) -e normal --prefix none -f '{p}' | sed 's/ (.*//' | sort -u \
		| awk '{print $$1}' | uniq -d | grep -E '^(arrow|parquet)' || true)"; \
	if [ -n "$$dups" ]; then echo "more than one version of: $$dups"; exit 1; fi; \
	$(CARGO) tree $(CRATE) -e normal --prefix none -f '{p}' | grep -E '^(arrow |arrow-array |parquet )' | sort -u

check: fmt-check clippy check-arrow test

clean:
	rm -f $(EXTENSION)
