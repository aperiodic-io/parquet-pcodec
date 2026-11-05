"""catalog: the provider samples, each under one data type."""

from __future__ import annotations

import pathlib

import pytest

from tpq_bench import catalog, fetch, report


def test_every_source_has_a_known_group_and_loader() -> None:
    for s in catalog.SOURCES:
        assert s.group in report.GROUPS, s.key
        assert s.loader in {"parquet", "tardis", "nyse_trades", "nyse_bbo", "dbn"}, s.key


def test_keys_are_unique_and_live_under_their_provider() -> None:
    keys = [s.key for s in catalog.SOURCES]
    assert len(keys) == len(set(keys))
    for s in catalog.SOURCES:
        assert s.key.split("/")[0] == catalog.slug(s.provider)


def test_the_providers() -> None:
    assert {s.provider for s in catalog.SOURCES} == {"Aperiodic", "Databento", "Tardis.dev", "Crypto Lake", "NYSE"}


def test_aperiodic_raw_and_metrics() -> None:
    ap = [s for s in catalog.SOURCES if s.provider == "Aperiodic"]
    assert {s.group for s in ap} == {"trades", "metrics", "bars"}
    raw = [s for s in ap if s.group == "trades"]
    assert len(raw) == 2
    assert all(isinstance(s.fetch, fetch.Http) and "download=true" in s.fetch.url for s in raw)
    metrics = [s for s in ap if s.group != "trades"]
    assert len(metrics) == len(catalog.APERIODIC_METRICS) == 19
    assert all(isinstance(s.fetch, fetch.SignedUrl) for s in metrics)
    assert all(dict(s.fetch.headers) == {"X-API-KEY": "DEMO-KEY"} for s in ap)  # type: ignore[attr-defined]


def test_only_databento_needs_a_key() -> None:
    for s in catalog.SOURCES:
        assert (s.needs == "DATABENTO_API_KEY") == (s.provider == "Databento"), s.key


@pytest.mark.parametrize(
    ("words", "providers"),
    [
        (["tardis"], {"Tardis.dev"}),
        (["Crypto Lake", "nyse"], {"Crypto Lake", "NYSE"}),
        (["l2"], {"Tardis.dev", "Crypto Lake", "Databento"}),
    ],
)
def test_select(words: list[str], providers: set[str]) -> None:
    assert {s.provider for s in catalog.select(catalog.SOURCES, words)} == providers


def test_select_by_key() -> None:
    [s] = catalog.select(catalog.SOURCES, ["aperiodic/metrics_flow_"])
    assert s.data.startswith("flow")


def test_select_nothing_is_everything() -> None:
    assert catalog.select(catalog.SOURCES, []) == catalog.SOURCES


def test_select_unknown() -> None:
    with pytest.raises(ValueError, match="matches no source"):
        catalog.select(catalog.SOURCES, ["bloomberg"])


@pytest.mark.parametrize(
    ("schema", "group"),
    [
        ("trades", "trades"),
        ("mbp-1", "quotes"),
        ("tbbo", "quotes"),
        ("bbo-1s", "quotes"),
        ("mbp-10", "l2"),
        ("mbo", "l3"),
        ("ohlcv-1m", "bars"),
    ],
)
def test_dbn_group(schema: str, group: str) -> None:
    assert catalog.dbn_group(schema) == group


def test_dbn_group_unknown() -> None:
    with pytest.raises(ValueError, match="statistics"):
        catalog.dbn_group("statistics")


def test_dbn_file_source(tmp_path: pathlib.Path) -> None:
    s = catalog.dbn_file(tmp_path / "glbx-mdp3-20250602.mbp-10.dbn.zst", "mbp-10")
    assert (s.provider, s.group, s.loader, s.data) == ("Databento", "l2", "dbn", "glbx-mdp3-20250602.mbp-10.dbn.zst")
