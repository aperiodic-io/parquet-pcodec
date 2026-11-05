"""cli: fetch, measure and report, skipping what cannot be fetched here."""

from __future__ import annotations

import json
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tpq_bench import catalog, cli, fetch


def trades(path: pathlib.Path) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 5000
    pq.write_table(
        pa.table({"ts": pa.array(range(0, 3 * n, 3), pa.int64()), "price": [100 + (i % 7) / 100 for i in range(n)]}),
        path,
    )
    return path


def test_run_measures_the_cached_sources_and_skips_the_unavailable(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data = tmp_path / "data"
    trades(data / "x" / "t.parquet")
    sources = [
        catalog.Source("X", "BTC trades", "trades", "x/t.parquet", "parquet", catalog.Local()),
        catalog.Source("Y", "ES trades", "trades", "y/t.dbn", "dbn", catalog.Local(), needs="Y_KEY"),
    ]
    entries, skipped = cli.run(sources, data)
    assert [(e.provider, e.data, e.m.rows) for e in entries] == [("X", "BTC trades", 5000)]
    assert entries[0].source_bytes == (data / "x" / "t.parquet").stat().st_size
    assert [s.provider for s, _ in skipped] == ["Y"]
    assert "does not exist" in skipped[0][1]
    # the measurements' files are gone
    assert sorted(p.relative_to(data).as_posix() for p in data.rglob("*")) == ["x", "x/t.parquet"]
    err = capsys.readouterr().err
    assert "X: BTC trades ..." in err
    assert "  5,000 rows, ZSTD(9) " in err
    assert ", tparquet " in err
    assert "×" in err
    assert "skipped: Y: ES trades: " in err


def test_main_writes_markdown_and_json(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = tmp_path / "data"
    trades(data / "x" / "t.parquet")
    monkeypatch.setattr(
        catalog, "SOURCES", [catalog.Source("X", "BTC trades", "trades", "x/t.parquet", "parquet", catalog.Local())]
    )
    md, js = tmp_path / "r.md", tmp_path / "r.json"
    assert cli.main(["run", "--data", str(data), "--markdown", str(md), "--json", str(js)]) == 0
    assert "| X | BTC trades | 5,000 |" in md.read_text()
    assert json.loads(js.read_text())[0]["rows"] == 5000


def test_main_fetch_reports_what_it_could_not_fetch(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        catalog, "SOURCES", [catalog.Source("X", "BTC trades", "trades", "x/t.parquet", "parquet", catalog.Local())]
    )
    assert cli.main(["fetch", "--data", str(tmp_path)]) == 1
    assert "not fetched: X: BTC trades" in capsys.readouterr().err


def test_main_list(tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["list", "--data", str(tmp_path), "nyse"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "trades  nyse/EQY_US_TAQ_NYSE_1_TRADES_20260701.gz  missing",
        "quotes  nyse/EQY_US_TAQ_NYSE_1_BBO_20260701.gz  missing",
    ]


def test_dbn_files_are_added(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(catalog, "SOURCES", [])
    f = tmp_path / "glbx.mbp-10.dbn.zst"

    def schema(path: pathlib.Path) -> str:
        assert path == f
        return "mbp-10"

    monkeypatch.setattr(cli, "dbn_schema", schema)
    seen: list[list[catalog.Source]] = []

    def run(sources: list[catalog.Source], data: pathlib.Path) -> tuple[list[object], list[object]]:
        seen.append(list(sources))
        return [], []

    monkeypatch.setattr(cli, "run", run)
    f.write_bytes(b"")
    # nothing measured (the run is faked): exit status 1
    assert cli.main(["run", "--data", str(tmp_path), "--dbn", str(f)]) == 1
    assert [(s.group, s.key) for s in seen[0]] == [("l2", str(f.resolve()))]


def test_run_works_in_the_cache_directory(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = tmp_path / "data"
    trades(data / "x" / "t.parquet")
    seen: list[pathlib.Path] = []
    from tpq_bench.measure import measure as real

    def spy(reader: pa.RecordBatchReader, workdir: pathlib.Path) -> object:
        seen.append(workdir)
        return real(reader, workdir)

    monkeypatch.setattr("tpq_bench.cli.measure", spy)
    cli.run([catalog.Source("X", "t", "trades", "x/t.parquet", "parquet", catalog.Local())], data)
    # on the cache's disk, which has room for files of the source's size
    assert seen[0].parent == data
    assert seen[0].name.startswith("work-")


def test_main_writes_markdown_to_standard_output(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    data = tmp_path / "data"
    trades(data / "x" / "t.parquet")
    monkeypatch.setattr(
        catalog, "SOURCES", [catalog.Source("X", "BTC trades", "trades", "x/t.parquet", "parquet", catalog.Local())]
    )
    assert cli.main(["run", "--data", str(data)]) == 0
    assert capsys.readouterr().out.startswith("| Data type |")


def test_main_fetch_downloads_into_a_new_cache(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = trades(tmp_path / "remote" / "t.parquet")
    monkeypatch.setattr(
        catalog,
        "SOURCES",
        [catalog.Source("X", "BTC trades", "trades", "x/t.parquet", "parquet", fetch.Http(source.as_uri()))],
    )
    data = tmp_path / "a" / "b"
    assert cli.main(["fetch", "--data", str(data)]) == 0
    assert (data / "x" / "t.parquet").read_bytes() == source.read_bytes()


def test_main_list_shows_what_is_cached(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    trades(tmp_path / "x" / "t.parquet")
    monkeypatch.setattr(
        catalog,
        "SOURCES",
        [
            catalog.Source("X", "BTC trades", "trades", "x/t.parquet", "parquet", catalog.Local()),
            catalog.Source("X", "BTC quotes", "quotes", "x/q.parquet", "parquet", catalog.Local()),
        ],
    )
    # the cache from the environment
    monkeypatch.setenv("TPQ_BENCH_DATA", str(tmp_path))
    assert cli.main(["list"]) == 0
    assert capsys.readouterr().out.splitlines() == ["trades  x/t.parquet  cached", "quotes  x/q.parquet  missing"]
