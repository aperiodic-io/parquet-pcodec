# tparquet on real market data

The [acceptance test](../rust/tparquet/tests/acceptance.rs) measures tparquet on synthetic data.
This benchmark measures it on the sample and preview data that market data providers publish
for evaluation, grouped by data type: trades, quotes, order books, bars, metrics and
derivatives.

```sh
make bench            # from the repository root: downloads the samples, writes RESULTS.md
```

## Results

From the last run (31 datasets, 151M rows, 1.2 GB downloaded). Every dataset is in
[`RESULTS.md`](RESULTS.md), and the codec and encoding of every column are in
[`results.json`](results.json).

| Data type | Datasets | Rows | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned |
|---|---:|---:|---:|---:|---:|---:|---:|
| Trades | 5 | 138.9M | 737.0 MB | 520.0 MB | 362.2 MB | **0.49×** | **0.70×** |
| Quotes (L1) | 3 | 6.8M | 80.6 MB | 63.3 MB | 51.0 MB | **0.63×** | **0.81×** |
| Order book (L2) | 3 | 5.5M | 126.4 MB | 109.2 MB | 58.6 MB | **0.46×** | **0.54×** |
| Bars (OHLCV) | 1 | 8.9K | 296 KB | 160 KB | 108 KB | **0.36×** | **0.67×** |
| Metrics (5m bars) | 18 | 161K | 10.5 MB | 6.4 MB | 5.8 MB | **0.55×** | **0.91×** |
| Derivatives | 1 | 119K | 2.2 MB | 1.6 MB | 882 KB | **0.40×** | **0.55×** |
| **All** | 31 | 151.5M | 957.1 MB | 700.7 MB | 478.6 MB | **0.50×** | **0.68×** |

- tparquet wins on every dataset, against both baselines. The range is 0.17× to 0.70× of
  untuned ZSTD(9), and 0.49× to 0.96× of tuned ZSTD(9).
- The order books gain the most: about half of tuned ZSTD(9), because every price and size
  level is a numeric column.
- The 5m metrics gain the least against tuned ZSTD(9) (0.91×). They are aggregates such as
  ratios, volatilities and percentages, often float32, and leave pcodec little structure to use.
- NYSE quotes gain less than the crypto quotes (0.88× of tuned). One file holds many
  symbols, interleaved in time, so prices jump between rows.
- Strings keep tparquet's base codec, ZSTD(3) with the default encoding. On string trade ids
  (Aperiodic, Tardis.dev) the tuned baseline's DELTA_BYTE_ARRAY is smaller, so tparquet's
  total there would improve with a better base encoding for strings.
- Writing tparquet is faster than writing ZSTD(9) on the trades (44 s against 64 s), and
  2.8× slower on the order books, whose 80 to 100 columns are all PCODEC. Reading takes 1.1×
  to 1.5× the time of the ZSTD(9) files.

## What is measured

Every dataset is read as its provider delivers it, typed as the provider documents it, and
written three times:

| File | Writer | Codecs and encodings |
|---|---|---|
| **ZSTD(9)** | tparquet's writer, `policy="base"` | ZSTD level 9 on every column, writer defaults otherwise (dictionary, then PLAIN): what you get without tuning |
| **ZSTD(9) tuned** | pyarrow | ZSTD level 9, and per column the smallest on the first batch of the writer's default (dictionary, then PLAIN) and the encoding Parquet has for the type: DELTA_BINARY_PACKED for integers and timestamps, BYTE_STREAM_SPLIT for floating point, DELTA_BYTE_ARRAY or DELTA_LENGTH_BYTE_ARRAY for strings. This is what a careful writer does, and how several providers' own files are written |
| **tparquet** | tparquet's writer, defaults | PCODEC or ZSTD level 3 per column, whichever is smaller on the first batch |

Each writer gets batches of 2²⁰ rows (one row group). The run checks that the three
files hold the same values (floats bit for bit), then times a full read of each with
tparquet's reader. The *As delivered* column is the size of the provider's file (CSV, gzip;
Parquet with their codec). Only the writers and the readers are timed, not the
parsing of the source. Times come from one run on one machine: compare them with each other,
not across machines.

## The data

| Provider | What | Format | Access |
|---|---|---|---|
| [Aperiodic](https://aperiodic.io) | raw trades of the BTC perpetual on Binance and OKX, 2025-06 (80M and 54M rows); the 19 metric datasets (flow, liquidity, impact, funding, basis...) for BTC-USDT on Binance, 5m bars, 2025-05 | Parquet | the free preview, with the public demo key |
| [Tardis.dev](https://tardis.dev) | Binance USDⓈ-M BTCUSDT, 2025-06-01: trades, quotes, 25-level book snapshots, derivative ticker | CSV, gzip | the first day of each month is free, no key |
| [Crypto Lake](https://crypto-lake.com) | Binance spot: BTC-USDT trades and top of book (2022-10-01), DOGE-USDT 20-level book snapshots (2024-09-24), AVAX-USDT book updates (2024-04-01) | Parquet | the public sample bucket (`lakeapi.use_sample_data(anonymous_access=True)`) |
| [NYSE](https://ftp.nyse.com/Historical%20Data%20Samples/) | TAQ NYSE Trades and BBO, 2026-07-01, symbol range 1 (US equities) | CSV, gzip | the public historical data samples |

About 1.2 GB is downloaded into `bench/data/` (git-ignored; `TPQ_BENCH_DATA` or `--data`
moves it). None of it is in the repository: the providers' terms do not allow redistributing
it, so every run downloads it from the provider.

## Running part of it

```sh
cd bench
uv sync
uv run --no-sync tpq-bench list                  # every source, and whether it is cached
uv run --no-sync tpq-bench fetch                 # download only
uv run --no-sync tpq-bench run l2 nyse           # data types, providers or parts of cache keys
uv run --no-sync tpq-bench run --markdown out.md --json out.json
```

## Development

```sh
uv run --no-sync pytest                          # offline: tiny synthetic files, no provider data
uv run --no-sync ruff check src tests && uv run --no-sync ruff format --check src tests
uv run --no-sync mypy
uv run --no-sync python scripts/mutmut_check.py     # fails unless every mutant is killed
```

New sources go in [`catalog.py`](src/tpq_bench/catalog.py), with a loader in
[`loaders.py`](src/tpq_bench/loaders.py) if the format is new.
