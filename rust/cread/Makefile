# Builds libtparquet_read and runs the plain-C smoke test against the static and the shared
# library.
ROOT := $(abspath ../..)
LIBDIR := $(ROOT)/target/release
CFLAGS ?= -O2
WARN := -std=c11 -Wall -Wextra -Werror -pedantic

.PHONY: lib c-test
lib:
	cargo build --release -p tparquet-cread

c-test: lib
	$(CC) $(CFLAGS) $(WARN) -I include tests/c/smoke.c $(LIBDIR)/libtparquet_read.a -lpthread -ldl -lm \
	  -o $(ROOT)/target/tpr-smoke-static
	$(ROOT)/target/tpr-smoke-static $(ROOT)/testdata
	$(CC) $(CFLAGS) $(WARN) -I include tests/c/smoke.c -L $(LIBDIR) -ltparquet_read -Wl,-rpath,$(LIBDIR) \
	  -o $(ROOT)/target/tpr-smoke-shared
	$(ROOT)/target/tpr-smoke-shared $(ROOT)/testdata
