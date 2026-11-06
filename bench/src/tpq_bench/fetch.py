"""Downloads into the cache: each file once, and never a partial file under its final name."""

from __future__ import annotations

import dataclasses
import json
import shutil
import urllib.request
from collections.abc import Callable
from typing import IO, TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pathlib

    from tpq_bench.catalog import Source

USER_AGENT = "tparquet-bench/0.1 (+https://github.com/aperiodic-io/tparquet)"
TIMEOUT = 60.0

Opener = Callable[..., IO[bytes]]


class Unavailable(Exception):
    """The source cannot be fetched here (no key, no file, no client library)."""


class Fetcher(Protocol):
    def __call__(self, dest: pathlib.Path, opener: Opener = ...) -> None: ...


def _get(url: str, headers: tuple[tuple[str, str], ...], opener: Opener) -> IO[bytes]:
    # urllib normalises the case of header names
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **dict(headers)})  # pragma: no mutate
    return opener(request, timeout=TIMEOUT)


def download(url: str, dest: pathlib.Path, headers: tuple[tuple[str, str], ...], opener: Opener) -> None:
    """Streams `url` to `dest` through `dest.part`, renamed once complete."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")  # pragma: no mutate (any temporary name)
    try:
        with _get(url, headers, opener) as response, open(part, "wb") as out:
            shutil.copyfileobj(response, out, 1 << 20)  # pragma: no mutate (buffer size: speed)
        part.replace(dest)
    finally:
        part.unlink(missing_ok=True)


@dataclasses.dataclass(frozen=True)
class Http:
    """A file at a URL (redirects followed)."""

    url: str
    headers: tuple[tuple[str, str], ...] = ()

    def __call__(self, dest: pathlib.Path, opener: Opener = urllib.request.urlopen) -> None:
        download(self.url, dest, self.headers, opener)


@dataclasses.dataclass(frozen=True)
class SignedUrl:
    """An API that answers `{"files": [{"url": ...}]}` with a short-lived signed URL of the file."""

    api: str
    headers: tuple[tuple[str, str], ...] = ()

    def __call__(self, dest: pathlib.Path, opener: Opener = urllib.request.urlopen) -> None:
        with _get(self.api, self.headers, opener) as response:
            files = json.load(response).get("files") or []
        if not files:
            raise Unavailable(f"{self.api} lists no file")
        # the signature is the credential of the storage host: the API's headers stay with the API
        download(files[0]["url"], dest, (), opener)


def ensure(source: Source, data_dir: pathlib.Path, opener: Opener = urllib.request.urlopen) -> pathlib.Path:
    """The cached file of `source`, downloaded first when missing."""
    path = data_dir / source.key
    if not path.exists():
        source.fetch(path, opener)
    return path
