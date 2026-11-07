# Proposal: PCODEC as a standard Apache Parquet compression codec

Status: draft, not yet submitted. This is the text the maintainers intend to bring to the Apache
Parquet community (dev@parquet.apache.org, then a pull request to `apache/parquet-format`). It
says what would change in the Parquet format, why, what is not settled, and what has to be
done first. [`FORMAT.md`](FORMAT.md) stays the specification of the codec as tparquet ships it
today; nothing here changes it.

## Why upstream

The one cost of tparquet that no amount of engineering in this repository removes is that a
reader without the codec cannot read the file (README, *It's still Parquet*). Today that
reader is almost every reader: pyarrow, pandas, Polars, DuckDB, Spark, Trino, parquet-java,
arrow-rs, and every managed engine whose Parquet reader nobody outside the vendor can patch.
The forks in this repository reach the open-source engines one pinned version at a time; they
cannot reach the managed ones at all.

A codec value in `parquet.thrift`, and the codec in the reference implementations, is the
only route by which a PCODEC file becomes readable by tools its writer does not control. Until
then tparquet is best used as a storage tier with a standard copy at its edges
([`docs/STORAGE-TIER.md`](../docs/STORAGE-TIER.md)).

## What would change in the format

`parquet.thrift`:

```thrift
enum CompressionCodec {
  UNCOMPRESSED = 0;
  SNAPPY = 1;
  GZIP = 2;
  LZO = 3;
  BROTLI = 4;
  LZ4 = 5;
  ZSTD = 6;
  LZ4_RAW = 7;
  PCODEC = 8;   // proposed
}
```

`Compression.md`: a `PCODEC` section that refers to the payload specification, which is
[`FORMAT.md`](FORMAT.md) sections 3, 4 and 7 with the codec value replaced (8 instead of
20547) and section 2's key-value pair dropped (a standard codec needs no hint to explain a
failure). The payload itself does not change: version byte, the RAW, PCO, DECIMAL and ZSTD
modes, and the rule that the decoded bytes must be exactly `uncompressed_page_size`.

Nothing else changes: no new page type, encoding, logical type or metadata field.

## Why a codec and not an encoding

Parquet separates encodings (typed, per value) from codecs (bytes in, bytes out). PCODEC is
specified as a codec because it meets the codec contract:

- **Decoding needs no type.** The payload names its own mode and number type; for any byte
  string `x`, `decompress(compress(x, hint), len(x)) == x` whatever the hint (FORMAT.md
  section 1). A reader calls it exactly as it calls ZSTD.
- **It applies where codecs apply**: the compressed part of V1 and V2 data pages and of
  dictionary pages (FORMAT.md section 3).
- **It degrades instead of failing.** A writer that does not pass the column type, or that
  uses a dictionary or a non-`PLAIN` encoding, still writes valid pages; the encoder falls back
  to its ZSTD or RAW mode.

What is unusual is that a writer gets the ratio only when it passes the column's physical type
as a hint. Reader APIs need nothing new; writer-side codec APIs in parquet-java and Arrow C++
take no type today, so the reference writers would need a small API change (the forks here
show its size: [`java/parquet-java/FORK.md`](../java/parquet-java/FORK.md),
[`cpp/arrow/FORK.md`](../cpp/arrow/FORK.md)).

The alternative is to split the work: the decimal-to-integer step (DECIMAL mode) as a new
encoding for `FLOAT` and `DOUBLE`, and pcodec alone as the codec. That is closer to Parquet's
layering and lets the decimal step be combined with other codecs, but it is two format changes
instead of one, and pcodec without the decimal step loses much of its advantage on prices and
sizes. The proposal should offer both and let the community choose.

## Evidence

From [`bench/RESULTS.md`](../bench/RESULTS.md): 151.5M rows in 31 datasets of four market
data providers, against Parquet with ZSTD level 9.

| | vs ZSTD(9) | vs ZSTD(9) with tuned encodings |
|---|---:|---:|
| Trades, 138.9M rows | 0.49× | 0.70× |
| Quotes, 6.8M rows | 0.63× | 0.81× |
| Order books, 5.5M rows | 0.46× | 0.54× |
| All | 0.50× | 0.68× |

All values round-trip bit for bit, and tparquet was smaller on every dataset. The proposal
must also state where it is weaker:

- **Read speed**: 925 MB/s of Arrow data against 1,031 for ZSTD(9) and 1,225 for tuned
  encodings, on one thread; on wide order books, about two thirds of ZSTD(9).
- **Write speed**: comparable on trades, about a third of ZSTD(9) on wide order books, much
  slower on small files.
- **Data it does not help**: strings and booleans; order books stored as lists come out at
  1.07× of ZSTD(9) (README, *Development*).
- **Narrow evidence**: every dataset is market data. The proposal needs measurements on other
  numeric data (sensors, metrics, scientific arrays, the community's usual benchmark files)
  before it is sent.

## What has to be done before submitting

1. **An independent implementation.** Every implementation in this repository (Rust, Go,
   Python, Java, C++, DuckDB, DataFusion, Trino) calls the same Rust core, `rust/core`, which
   uses the `pco` crate. The Parquet community expects a format feature to be implemented in
   the reference libraries, and parquet-java and Arrow C++ are unlikely to accept a Rust build
   dependency. At least a decoder written from the specification alone, in Java or C++, is
   needed: it shows that the specification is complete and gives those libraries a path
   without Rust. It must pass every vector in [`testdata/`](../testdata/README.md).
2. **A fixed pcodec format reference.** FORMAT.md defers the `pco` body to "pcodec ≥ 1.0
   standalone format". An Apache specification cannot point at a moving project: the proposal
   has to cite a specific, written version of pcodec's format, and the independent decoder in
   step 1 must be written from that document.
3. **Broader benchmarks**, with the weaknesses above stated, as described under *Evidence*.
4. **A comparison with the other proposals.** The community has discussed new encodings for
   floating-point data (such as ALP), which also turn decimal floats into integers. The
   proposal should measure against whichever of them have a public implementation, and say
   whether PCODEC competes with them or complements them.
5. **A discussion on dev@parquet.apache.org** before any pull request, following the process
   in `apache/parquet-format`'s contributing guide at that time.

## Existing files

Files written today carry codec value 20547. If the community assigns a value:

- tparquet readers accept both values for as long as files with 20547 may exist; the payload
  is identical, so both decode the same way.
- tparquet writers keep writing 20547 until releases of the reference libraries can read the
  new value, then switch, with an option to keep the old value.
- The pages of an existing file need no rewrite: only the codec values in the metadata differ.
  A tool that rewrites them in place is future work; until then `tparquet decode` followed by
  an encode with the new value converts a file.

If the community chooses the split (an encoding plus a codec), existing files stay readable by
tparquet and are converted with `tparquet decode` and a new encode.

## Open questions

- Codec, or encoding plus codec (see above)?
- Should the ZSTD mode stay? It makes PCODEC total over any page (a writer can set it on any
  column), but duplicates the ZSTD codec. Dropping it makes non-numeric pages RAW or invalid.
- Is the type hint acceptable as a writer-side codec parameter, or should the codec take the
  physical type from the column metadata, which a reader also has?
- Level: pcodec's level 0–12 (default 8) is not recorded in the file, as ZSTD's is not. Is a
  recommended default part of the specification?
