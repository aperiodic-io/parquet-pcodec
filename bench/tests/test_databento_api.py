"""databento_api: a key, a price check, then the download; Databento's client faked."""

from __future__ import annotations

import pathlib
import sys
import types
from typing import Any

import pytest

from tpq_bench import fetch
from tpq_bench.databento_api import Databento

REQUEST = Databento("GLBX.MDP3", "ES.c.0", "continuous", schema="mbo", start="2025-06-02T13:30", end="2025-06-02T14:30")
ARGS = {
    "dataset": "GLBX.MDP3",
    "symbols": "ES.c.0",
    "stype_in": "continuous",
    "schema": "mbo",
    "start": "2025-06-02T13:30",
    "end": "2025-06-02T14:30",
}


class Client:
    """Databento's `Historical`, recording the calls."""

    calls: list[tuple[str, dict[str, Any]]]

    def __init__(self, cost: float, fail: bool = False) -> None:
        self.cost, self.fail, self.calls = cost, fail, []
        self.metadata = types.SimpleNamespace(get_cost=self.get_cost)
        self.timeseries = types.SimpleNamespace(get_range=self.get_range)

    def get_cost(self, **kwargs: Any) -> float:
        self.calls.append(("get_cost", kwargs))
        return self.cost

    def get_range(self, path: pathlib.Path, **kwargs: Any) -> None:
        self.calls.append(("get_range", {**kwargs, "path": path}))
        path.write_bytes(b"DBN")
        if self.fail:
            raise OSError("connection reset")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Client:
    c = Client(cost=0.5)
    keys: list[str] = []

    def historical(key: str) -> Client:
        keys.append(key)
        return c

    monkeypatch.setitem(sys.modules, "databento", types.SimpleNamespace(Historical=historical))
    monkeypatch.setenv("DATABENTO_API_KEY", "db-test")
    monkeypatch.delenv("TPQ_BENCH_DATABENTO_MAX_COST", raising=False)
    c.keys = keys  # type: ignore[attr-defined]
    return c


def test_no_key(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    with pytest.raises(
        fetch.Unavailable, match=r"^set DATABENTO_API_KEY to fetch from Databento's API, or pass DBN files with --dbn$"
    ):
        REQUEST(tmp_path / "x.dbn.zst")


def test_no_client(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "db-test")
    monkeypatch.setitem(sys.modules, "databento", None)  # import fails
    with pytest.raises(fetch.Unavailable, match=r"^the Databento client is missing: install tpq-bench\[databento\]$"):
        REQUEST(tmp_path / "x.dbn.zst")


def test_download(client: Client, tmp_path: pathlib.Path) -> None:
    dest = tmp_path / "databento" / "new" / "x.dbn.zst"
    REQUEST(dest)
    assert dest.read_bytes() == b"DBN"
    assert list(dest.parent.iterdir()) == [dest]
    assert client.keys == ["db-test"]  # type: ignore[attr-defined]
    assert client.calls == [("get_cost", ARGS), ("get_range", {**ARGS, "path": dest.with_name("x.dbn.zst.part")})]


def test_too_expensive(client: Client, tmp_path: pathlib.Path) -> None:
    client.cost = 2.01
    with pytest.raises(
        fetch.Unavailable,
        match=r"^mbo would cost \$2\.01, above the \$2\.00 limit \(TPQ_BENCH_DATABENTO_MAX_COST raises it\)$",
    ):
        REQUEST(tmp_path / "x.dbn.zst")
    assert [name for name, _ in client.calls] == ["get_cost"]


def test_at_the_limit(client: Client, tmp_path: pathlib.Path) -> None:
    client.cost = 2.0
    REQUEST(tmp_path / "x.dbn.zst")
    assert (tmp_path / "x.dbn.zst").exists()


def test_the_limit_is_configurable(client: Client, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TPQ_BENCH_DATABENTO_MAX_COST", "0.25")
    with pytest.raises(fetch.Unavailable, match=r"above the \$0\.25 limit"):
        REQUEST(tmp_path / "x.dbn.zst")


def test_failed_download_leaves_nothing(client: Client, tmp_path: pathlib.Path) -> None:
    client.fail = True
    with pytest.raises(OSError, match="connection reset"):
        REQUEST(tmp_path / "x.dbn.zst")
    assert list(tmp_path.iterdir()) == []
