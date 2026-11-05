# Results

The last run of `make bench` (2026-11-05; tparquet fec98be, pyarrow 26.0.1,
Python 3.14.6, on a 4-vCPU cloud VM). How it is measured: [README.md](README.md#what-is-measured).
Ratios below 1 mean tparquet is smaller.

| Data type | Datasets | Rows | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | Write s: ZSTD(9) / tuned / tparquet | Read s: ZSTD(9) / tuned / tparquet |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Trades | 5 | 138,865,292 | 737.0 MB | 520.0 MB | 362.2 MB | **0.491×** | **0.696×** | 64.16 / 42.65 / 44.12 | 6.74 / 5.56 / 7.40 |
| Quotes (L1, top of book) | 3 | 6,832,005 | 80.6 MB | 63.3 MB | 51.0 MB | **0.633×** | **0.806×** | 4.71 / 2.68 / 6.14 | 0.51 / 0.34 / 0.47 |
| Order book (L2: snapshots and updates) | 3 | 5,513,803 | 126.4 MB | 109.2 MB | 58.6 MB | **0.464×** | **0.537×** | 9.09 / 9.79 / 25.57 | 1.05 / 1.07 / 1.58 |
| Bars (OHLCV) | 1 | 8,928 | 296 KB | 160 KB | 108 KB | **0.364×** | **0.673×** | 0.01 / 0.01 / 0.05 | 0.00 / 0.00 / 0.00 |
| Metrics (5m bars) | 18 | 160,704 | 10.5 MB | 6.4 MB | 5.8 MB | **0.551×** | **0.905×** | 0.17 / 0.12 / 1.08 | 0.04 / 0.02 / 0.02 |
| Derivatives (funding, mark, index, open interest) | 1 | 118,524 | 2.2 MB | 1.6 MB | 882 KB | **0.395×** | **0.553×** | 0.10 / 0.08 / 0.25 | 0.01 / 0.01 / 0.01 |
| **All** | 31 | 151,499,256 | 957.1 MB | 700.7 MB | 478.6 MB | **0.500×** | **0.683×** | 78.24 / 55.33 / 77.21 | 8.35 / 7.01 / 9.48 |

### Trades

| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Aperiodic | BTC-USDT perpetual trades on Binance, 2025-06 (raw, Parquet) | 79,684,576 | 248.6 MB | 399.4 MB | 280.2 MB | 193.1 MB | 0.484× | 0.689× | 4 of 6 |
| Aperiodic | BTC-USDT perpetual trades on OKX, 2025-06 (raw, Parquet) | 54,282,320 | 189.1 MB | 294.8 MB | 208.5 MB | 147.1 MB | 0.499× | 0.706× | 4 of 6 |
| Tardis.dev | trades, Binance USDⓈ-M BTCUSDT, 2025-06-01 (CSV) | 1,959,946 | 17.8 MB | 11.0 MB | 7.5 MB | 5.2 MB | 0.474× | 0.691× | 4 of 8 |
| Crypto Lake | trades, Binance BTC-USDT, 2022-10-01 (Parquet) | 2,190,208 | 63.4 MB | 22.5 MB | 17.4 MB | 11.9 MB | 0.529× | 0.685× | 5 of 6 |
| NYSE | TAQ NYSE trades, symbol range 1, 2026-07-01 (CSV) | 748,242 | 12.9 MB | 9.3 MB | 6.4 MB | 4.8 MB | 0.513× | 0.748× | 6 of 11 |

### Quotes (L1, top of book)

| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Tardis.dev | quotes, Binance USDⓈ-M BTCUSDT, 2025-06-01 (CSV) | 1,210,001 | 16.3 MB | 13.4 MB | 8.9 MB | 5.1 MB | 0.380× | 0.573× | 6 of 8 |
| Crypto Lake | top of book, Binance BTC-USDT, 2022-10-01 (Parquet) | 713,858 | 11.2 MB | 7.8 MB | 7.4 MB | 4.5 MB | 0.574× | 0.610× | 5 of 5 |
| NYSE | TAQ NYSE BBO quotes, symbol range 1, 2026-07-01 (CSV) | 4,908,146 | 74.6 MB | 59.4 MB | 47.0 MB | 41.4 MB | 0.698× | 0.881× | 7 of 10 |

### Order book (L2: snapshots and updates)

| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Tardis.dev | 25-level book snapshots, Binance USDⓈ-M BTCUSDT, 2025-06-01 (CSV) | 1,429,796 | 53.8 MB | 81.5 MB | 75.9 MB | 38.2 MB | 0.468× | 0.503× | 102 of 104 |
| Crypto Lake | 20-level book snapshots, Binance DOGE-USDT, 2024-09-24 (Parquet) | 140,676 | 27.2 MB | 15.2 MB | 11.1 MB | 5.7 MB | 0.374× | 0.513× | 83 of 83 |
| Crypto Lake | book updates, Binance AVAX-USDT, 2024-04-01 (Parquet) | 3,943,331 | 46.4 MB | 29.7 MB | 22.2 MB | 14.8 MB | 0.497× | 0.665× | 5 of 6 |

### Bars (OHLCV)

| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Aperiodic | ohlcv, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 159 KB | 296 KB | 160 KB | 108 KB | 0.364× | 0.673× | 7 of 7 |

### Metrics (5m bars)

| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Aperiodic | vtwap, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 108 KB | 205 KB | 109 KB | 101 KB | 0.494× | 0.928× | 3 of 3 |
| Aperiodic | basis, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 223 KB | 390 KB | 224 KB | 204 KB | 0.522× | 0.909× | 9 of 9 |
| Aperiodic | funding, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 26 KB | 80 KB | 27 KB | 13 KB | 0.166× | 0.493× | 3 of 4 |
| Aperiodic | open_interest, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 272 KB | 391 KB | 266 KB | 227 KB | 0.582× | 0.855× | 6 of 6 |
| Aperiodic | derivative_price, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 512 KB | 696 KB | 513 KB | 445 KB | 0.640× | 0.869× | 10 of 10 |
| Aperiodic | flow, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 789 KB | 1.3 MB | 786 KB | 733 KB | 0.562× | 0.932× | 37 of 37 |
| Aperiodic | trade_size, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 556 KB | 960 KB | 554 KB | 520 KB | 0.542× | 0.939× | 25 of 25 |
| Aperiodic | impact, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 157 KB | 285 KB | 158 KB | 148 KB | 0.520× | 0.938× | 6 of 6 |
| Aperiodic | range, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 302 KB | 509 KB | 304 KB | 271 KB | 0.533× | 0.892× | 11 of 11 |
| Aperiodic | updownticks, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 364 KB | 629 KB | 362 KB | 319 KB | 0.507× | 0.881× | 15 of 15 |
| Aperiodic | run_structure, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 182 KB | 309 KB | 176 KB | 160 KB | 0.519× | 0.913× | 8 of 8 |
| Aperiodic | returns, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 181 KB | 331 KB | 182 KB | 174 KB | 0.528× | 0.960× | 7 of 7 |
| Aperiodic | slippage, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 275 KB | 478 KB | 276 KB | 261 KB | 0.547× | 0.947× | 11 of 11 |
| Aperiodic | l1_price, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 488 KB | 668 KB | 443 KB | 345 KB | 0.516× | 0.779× | 13 of 13 |
| Aperiodic | l1_imbalance, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 307 KB | 518 KB | 308 KB | 275 KB | 0.530× | 0.892× | 11 of 11 |
| Aperiodic | l1_liquidity, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 361 KB | 636 KB | 363 KB | 335 KB | 0.526× | 0.921× | 15 of 15 |
| Aperiodic | l2_imbalance, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 760 KB | 1.2 MB | 764 KB | 713 KB | 0.594× | 0.934× | 25 of 25 |
| Aperiodic | l2_liquidity, BTC-USDT perpetual on Binance, 5m, 2025-05 | 8,928 | 598 KB | 942 KB | 601 KB | 558 KB | 0.592× | 0.928× | 17 of 17 |

### Derivatives (funding, mark, index, open interest)

| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Tardis.dev | derivative ticker, Binance USDⓈ-M BTCUSDT, 2025-06-01 (CSV) | 118,524 | 1.8 MB | 2.2 MB | 1.6 MB | 882 KB | 0.395× | 0.553× | 8 of 11 |
