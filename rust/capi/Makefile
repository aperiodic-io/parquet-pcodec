# Builds the static library and runs the plain-C smoke test.
ROOT := $(abspath ../..)
LIB := $(ROOT)/target/release/libtparquet_capi.a

.PHONY: lib c-test
lib:
	cargo build --release -p tparquet-capi

c-test: lib
	$(CC) -O2 -Wall -Wextra -Werror -I include tests/c/smoke.c $(LIB) -lpthread -ldl -lm -o $(ROOT)/target/pcp-smoke
	$(ROOT)/target/pcp-smoke
