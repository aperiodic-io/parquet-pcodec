# tparquet on real market data

The [acceptance test](../rust/tparquet/tests/acceptance.rs) measures tparquet on synthetic data.
This benchmark measures it on the sample and preview data that market data providers publish
for evaluation, grouped by data type: trades, quotes, order books, bars, metrics and
derivatives.

```sh
make bench            # from the repository root: downloads the samples, writes RESULTS.md
```

The results of the last run are in [`RESULTS.md`](RESULTS.md) (machine-readable:
[`results.json`](results.json)).

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
Parquet with their codec; DBN with zstd). Only the writers and the readers are timed, not the
parsing of the source. Times come from one run on one machine: compare them with each other,
not across machines.

## The data

| Provider | What | Format | Access |
|---|---|---|---|
| [Aperiodic](https://aperiodic.io) | raw trades of the BTC perpetual on Binance and OKX, 2025-06 (80M and 54M rows); the 19 metric datasets (flow, liquidity, impact, funding, basis...) for BTC-USDT on Binance, 5m bars, 2025-05 | Parquet | the free preview, with the public demo key |
| [Databento](https://databento.com) | CME E-mini S&P 500 front month: trades, MBP-1, MBP-10, MBO, 1m bars | DBN | its API, with your key (new accounts get free credits), or DBN files from its portal |
| [Tardis.dev](https://tardis.dev) | Binance USDⓈ-M BTCUSDT, 2025-06-01: trades, quotes, 25-level book snapshots, derivative ticker | CSV, gzip | the first day of each month is free, no key |
| [Crypto Lake](https://crypto-lake.com) | Binance spot: BTC-USDT trades and top of book (2022-10-01), DOGE-USDT 20-level book snapshots (2024-09-24), AVAX-USDT book updates (2024-04-01) | Parquet | the public sample bucket (`lakeapi.use_sample_data(anonymous_access=True)`) |
| [NYSE](https://ftp.nyse.com/Historical%20Data%20Samples/) | TAQ NYSE Trades and BBO, 2026-07-01, symbol range 1 (US equities) | CSV, gzip | the public historical data samples |

About 1.2 GB is downloaded into `bench/data/` (git-ignored; `TPQ_BENCH_DATA` or `--data`
moves it). None of it is in the repository: the providers' terms do not allow redistributing
it, so every run downloads it from the provider.

### Databento

Databento's samples need an account. Either export an API key (the run checks the price of
each request first and refuses one above $2, `TPQ_BENCH_DATABENTO_MAX_COST` changes that):

```sh
DATABENTO_API_KEY=db-... make bench
```

or download DBN files from the portal (each dataset page has sample data) and pass them:

```sh
cd bench && uv run --no-sync tpq-bench run --dbn ~/Downloads/*.dbn.zst
```

Without either, the Databento rows are skipped.

## Running part of it

```sh
cd bench
uv sync --extra databento
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
uv run --no-sync mutmut run
```

New sources go in [`catalog.py`](src/tpq_bench/catalog.py), with a loader in
[`loaders.py`](src/tpq_bench/loaders.py) if the format is new.
