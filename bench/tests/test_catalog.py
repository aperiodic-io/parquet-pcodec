"""catalog: the provider samples, each under one data type."""

from __future__ import annotations

import pytest

from tpq_bench import catalog, fetch, report


def test_every_source_has_a_known_group_and_loader() -> None:
    for s in catalog.SOURCES:
        assert s.group in report.GROUPS, s.key
        assert s.loader in {"parquet", "tardis", "nyse_trades", "nyse_bbo"}, s.key


def test_keys_are_unique_and_live_under_their_provider() -> None:
    keys = [s.key for s in catalog.SOURCES]
    assert len(keys) == len(set(keys))
    for s in catalog.SOURCES:
        assert s.key.split("/")[0] == catalog.slug(s.provider)


def test_the_providers() -> None:
    assert {s.provider for s in catalog.SOURCES} == {"Aperiodic", "Tardis.dev", "Crypto Lake", "NYSE"}


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


@pytest.mark.parametrize(
    ("words", "providers"),
    [
        (["tardis"], {"Tardis.dev"}),
        (["Crypto Lake", "nyse"], {"Crypto Lake", "NYSE"}),
        (["l2"], {"Tardis.dev", "Crypto Lake"}),
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
