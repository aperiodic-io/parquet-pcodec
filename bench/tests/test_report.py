"""report: results grouped by data type, as Markdown and JSON."""

from __future__ import annotations

import json

import pytest

from tpq_bench import report
from tpq_bench.measure import Measurement


def m(rows: int, z9: int, tpq: int, codecs: dict[str, str] | None = None) -> Measurement:
    return Measurement(
        rows=rows,
        zstd9_bytes=z9,
        tuned_bytes=z9 * 4 // 5,
        tparquet_bytes=tpq,
        write_s_zstd9=2.0,
        write_s_tuned=1.5,
        write_s_tparquet=1.0,
        read_s_zstd9=0.5,
        read_s_tuned=0.5,
        read_s_tparquet=0.25,
        codecs=codecs or {"ts": "PCODEC", "price": "PCODEC", "side": "ZSTD"},
        encodings={"ts": "DELTA_BINARY_PACKED", "price": "dictionary", "side": "dictionary"},
    )


ENTRIES = [
    report.Entry("Tardis.dev", "BTCUSDT quotes", "quotes", 17_800_000, m(1_000_000, 12_000_000, 6_000_000)),
    report.Entry("Tardis.dev", "BTCUSDT trades", "trades", 999, m(1_500, 400_000, 100_000)),
    report.Entry("NYSE", "TAQ trades", "trades", 13_000_000, m(2_500_000, 3_600_000_000, 1_600_000_000)),
]


@pytest.mark.parametrize(
    ("n", "text"),
    [
        (999, "999 B"),
        (1_000, "1 KB"),
        (361_499, "361 KB"),
        (999_000, "999 KB"),
        (1_000_000, "1.0 MB"),
        (12_345_678, "12.3 MB"),
        (100_050_050, "100.1 MB"),
        (1_000_000_000, "1.00 GB"),
        (2_005_000_001, "2.01 GB"),
        (3_600_000_000, "3.60 GB"),
    ],
)
def test_size(n: int, text: str) -> None:
    assert report.size(n) == text


def test_groups_follow_the_order_of_the_data_types() -> None:
    assert [g for g, _ in report.groups(ENTRIES)] == ["trades", "quotes"]
    assert [e.data for e in dict(report.groups(ENTRIES))["trades"]] == ["BTCUSDT trades", "TAQ trades"]


def test_unknown_group_is_refused() -> None:
    with pytest.raises(ValueError, match="group"):
        report.groups([report.Entry("X", "y", "weather", 1, m(1, 1, 1))])


def test_markdown() -> None:
    assert report.markdown(ENTRIES) == (
        "| Data type | Datasets | Rows | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned "
        "| Write s: ZSTD(9) / tuned / tparquet | Read s: ZSTD(9) / tuned / tparquet |\n"
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
        "| Trades | 2 | 2,501,500 | 3.60 GB | 2.88 GB | 1.60 GB | **0.444×** | **0.556×** | 4.00 / 3.00 / 2.00 | 1.00 / 1.00 / 0.50 |\n"
        "| Quotes (L1, top of book) | 1 | 1,000,000 | 12.0 MB | 9.6 MB | 6.0 MB | **0.500×** | **0.625×** | 2.00 / 1.50 / 1.00 | 0.50 / 0.50 / 0.25 |\n"
        "| **All** | 3 | 3,501,500 | 3.61 GB | 2.89 GB | 1.61 GB | **0.445×** | **0.556×** | 6.00 / 4.50 / 3.00 | 1.50 / 1.50 / 0.75 |\n"
        "\n"
        "### Trades\n"
        "\n"
        "| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n"
        "| Tardis.dev | BTCUSDT trades | 1,500 | 999 B | 400 KB | 320 KB | 100 KB | 0.250× | 0.312× | 2 of 3 |\n"
        "| NYSE | TAQ trades | 2,500,000 | 13.0 MB | 3.60 GB | 2.88 GB | 1.60 GB | 0.444× | 0.556× | 2 of 3 |\n"
        "\n"
        "### Quotes (L1, top of book)\n"
        "\n"
        "| Provider | Data | Rows | As delivered | ZSTD(9) | ZSTD(9) tuned | tparquet | tparquet / ZSTD(9) | tparquet / tuned | PCODEC columns |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n"
        "| Tardis.dev | BTCUSDT quotes | 1,000,000 | 17.8 MB | 12.0 MB | 9.6 MB | 6.0 MB | 0.500× | 0.625× | 2 of 3 |\n"
    )


def test_json_round_trips_every_field() -> None:
    rows = json.loads(report.to_json(ENTRIES[:1]))
    assert rows == [
        {
            "provider": "Tardis.dev",
            "data": "BTCUSDT quotes",
            "group": "quotes",
            "source_bytes": 17_800_000,
            "rows": 1_000_000,
            "zstd9_bytes": 12_000_000,
            "tuned_bytes": 9_600_000,
            "tparquet_bytes": 6_000_000,
            "ratio": 0.5,
            "ratio_tuned": 0.625,
            "write_s_zstd9": 2.0,
            "write_s_tuned": 1.5,
            "write_s_tparquet": 1.0,
            "read_s_zstd9": 0.5,
            "read_s_tuned": 0.5,
            "read_s_tparquet": 0.25,
            "codecs": {"ts": "PCODEC", "price": "PCODEC", "side": "ZSTD"},
            "tuned_encodings": {"ts": "DELTA_BINARY_PACKED", "price": "dictionary", "side": "dictionary"},
        }
    ]


def test_json_is_indented_and_keeps_unicode() -> None:
    entry = report.Entry("Tardis.dev", "BTCUSDT, Binance USDⓈ-M", "trades", 1, m(10, 10, 5))
    text = report.to_json([entry])
    assert text.startswith('[\n  {\n    "provider": "Tardis.dev",\n')
    assert '"data": "BTCUSDT, Binance USDⓈ-M"' in text
    assert text.endswith("}\n]\n")
