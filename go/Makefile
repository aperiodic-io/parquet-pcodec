# Builds and checks the Go package.
#
#   make test                         build ../target/release/libparquet_pcodec.a, run the tests
#   make test PCODEC_LIB_DIR=/opt/lib  link /opt/lib/libparquet_pcodec.a instead (no cargo)
#   make check                        vet, staticcheck, race tests and a short fuzz run
#   make mutation                     gremlins (config in .gremlins.yaml)
#
# With PCODEC_LIB_DIR the package is built with the pcodec_external tag and the library is
# passed in CGO_LDFLAGS: cgo directives cannot read environment variables.

ROOT := $(abspath ..)
GO ?= go
CARGO ?= cargo
FUZZTIME ?= 30s
STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@v0.7.0
GREMLINS ?= github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0

ifdef PCODEC_LIB_DIR
LIBA := $(abspath $(PCODEC_LIB_DIR))/libparquet_pcodec.a
TAGS := -tags pcodec_external
DEPS :=
else
LIBA := $(ROOT)/target/release/libparquet_pcodec.a
TAGS :=
DEPS := lib
endif

.PHONY: lib vet staticcheck test race fuzz mutation check vectors

lib:
	cd $(ROOT) && $(CARGO) build --release -p parquet-pcodec-capi

vet: $(DEPS)
	$(GO) vet $(TAGS) ./...

staticcheck: $(DEPS)
	$(GO) run $(STATICCHECK) $(TAGS) ./...

test: $(DEPS)
	$(GO) test $(TAGS) ./...

race: $(DEPS)
	$(GO) test $(TAGS) -race ./...

fuzz: $(DEPS)
	$(GO) test $(TAGS) -run '^$$' -fuzz '^FuzzDecode$$' -fuzztime $(FUZZTIME) .
	$(GO) test $(TAGS) -run '^$$' -fuzz '^FuzzRoundTrip$$' -fuzztime $(FUZZTIME) .

# gremlins tests a copy of the module, where ../target does not exist: always link by path.
mutation: $(DEPS)
	CGO_LDFLAGS=$(LIBA) $(GO) run $(GREMLINS) unleash .

# Go-produced conformance vectors, for the other implementations to read.
vectors: $(DEPS)
	$(GO) run $(TAGS) ./cmd/pcodec-go-gen $(or $(OUT),$(ROOT)/testdata)

check: vet staticcheck race fuzz

ifdef PCODEC_LIB_DIR
export CGO_LDFLAGS := $(LIBA)
endif
