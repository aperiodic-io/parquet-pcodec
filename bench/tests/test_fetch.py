"""fetch: downloads into the cache, once, never leaving a partial file behind."""

from __future__ import annotations

import io
import json
import pathlib
import urllib.request
from collections.abc import Callable
from typing import IO

import pytest

from tpq_bench import fetch
from tpq_bench.catalog import Source


def fake(responses: dict[str, bytes], seen: list[urllib.request.Request]) -> Callable[..., IO[bytes]]:
    def opener(request: urllib.request.Request, timeout: float | None = None) -> IO[bytes]:
        seen.append(request)
        if request.full_url not in responses:
            raise OSError(f"no {request.full_url}")
        return io.BytesIO(responses[request.full_url])

    return opener


def test_download_writes_the_file(tmp_path: pathlib.Path) -> None:
    seen: list[urllib.request.Request] = []
    dest = tmp_path / "a" / "b.csv.gz"
    fetch.Http("https://x/b", headers=(("X-API-KEY", "k"),))(dest, fake({"https://x/b": b"data"}, seen))
    assert dest.read_bytes() == b"data"
    assert seen[0].get_header("X-api-key") == "k"
    assert seen[0].get_header("User-agent") == fetch.USER_AGENT
    assert list(dest.parent.iterdir()) == [dest]


def test_failed_download_leaves_nothing(tmp_path: pathlib.Path) -> None:
    class Broken(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            raise OSError("reset")

    def opener(request: urllib.request.Request, timeout: float | None = None) -> IO[bytes]:
        return Broken(b"")

    dest = tmp_path / "b.parquet"
    with pytest.raises(OSError, match="reset"):
        fetch.Http("https://x/b")(dest, opener)
    assert list(tmp_path.iterdir()) == []


def test_signed_url_is_resolved_first(tmp_path: pathlib.Path) -> None:
    seen: list[urllib.request.Request] = []
    api = "https://aperiodic.io/api/v1/data/preview/flow?interval=5m"
    responses = {
        api: json.dumps({"files": [{"url": "https://r2/flow.parquet?sig=1"}]}).encode(),
        "https://r2/flow.parquet?sig=1": b"PAR1",
    }
    dest = tmp_path / "flow.parquet"
    fetch.SignedUrl(api, headers=(("X-API-KEY", "DEMO-KEY"),))(dest, fake(responses, seen))
    assert dest.read_bytes() == b"PAR1"
    assert [r.full_url for r in seen] == [api, "https://r2/flow.parquet?sig=1"]
    assert seen[0].get_header("X-api-key") == "DEMO-KEY"
    # the signature is the credential: the key is not sent to the storage host
    assert seen[1].get_header("X-api-key") is None


def test_signed_url_without_files(tmp_path: pathlib.Path) -> None:
    opener = fake({"https://a/p": b'{"files": []}'}, [])
    with pytest.raises(fetch.Unavailable, match="no file"):
        fetch.SignedUrl("https://a/p")(tmp_path / "x", opener)


def source(fetcher: fetch.Fetcher) -> Source:
    return Source("Tardis.dev", "trades", "trades", "tardis/t.csv.gz", "tardis", fetcher)


def test_ensure_downloads_once(tmp_path: pathlib.Path) -> None:
    seen: list[urllib.request.Request] = []
    opener = fake({"https://x/t": b"abc"}, seen)
    s = source(fetch.Http("https://x/t"))
    path = fetch.ensure(s, tmp_path, opener)
    assert path == tmp_path / "tardis" / "t.csv.gz"
    assert path.read_bytes() == b"abc"
    assert fetch.ensure(s, tmp_path, opener) == path
    assert len(seen) == 1
