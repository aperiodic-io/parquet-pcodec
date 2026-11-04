# Builds and checks the Go package.
#
#   make test                         build ../target/release/libpparquet_capi.a, run the tests
#   make test PPARQUET_LIB_DIR=/opt/lib  link /opt/lib/libpparquet_capi.a instead (no cargo)
#   make check                        vet, staticcheck, race tests and a short fuzz run
#   make mutation                     gremlins (config in .gremlins.yaml)
#
# With PPARQUET_LIB_DIR the package is built with the pparquet_external tag and the library is
# passed in CGO_LDFLAGS: cgo directives cannot read environment variables.

ROOT := $(abspath ..)
GO ?= go
CARGO ?= cargo
FUZZTIME ?= 30s
STATICCHECK ?= honnef.co/go/tools/cmd/staticcheck@v0.7.0
GREMLINS ?= github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0

ifdef PPARQUET_LIB_DIR
LIBA := $(abspath $(PPARQUET_LIB_DIR))/libpparquet_capi.a
TAGS := -tags pparquet_external
DEPS :=
else
LIBA := $(ROOT)/target/release/libpparquet_capi.a
TAGS :=
DEPS := lib
endif

.PHONY: lib vet staticcheck test race fuzz mutation check vectors

lib:
	cd $(ROOT) && $(CARGO) build --release -p pparquet-capi

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
# -count=1: gremlins sets the mutant timeouts from its coverage run, which must not be cached.
mutation: $(DEPS)
	CGO_LDFLAGS=$(LIBA) GOFLAGS="$(GOFLAGS) -count=1" $(GO) run $(GREMLINS) unleash .

# Go-produced conformance vectors, for the other implementations to read.
vectors: $(DEPS)
	$(GO) run $(TAGS) ./cmd/pparquet-go-gen $(or $(OUT),$(ROOT)/testdata)

check: vet staticcheck race fuzz

ifdef PPARQUET_LIB_DIR
export CGO_LDFLAGS := $(LIBA)
endif
