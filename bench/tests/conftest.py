"""Every test runs offline, with the cache in its own temporary directory."""

from __future__ import annotations

import dataclasses
import socket
from typing import TYPE_CHECKING, Any

import pytest

from tpq_bench.fetch import Unavailable

if TYPE_CHECKING:
    import pathlib


@dataclasses.dataclass(frozen=True)
class Cached:
    """The fetcher of a source whose file is put in the cache by the test itself."""

    def __call__(self, dest: pathlib.Path, opener: Any = None) -> None:
        raise Unavailable(f"{dest} does not exist")


@pytest.fixture(autouse=True)
def offline(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """No test reaches a provider, even a mutated one: sockets cannot connect (file URLs still
    open), and the default cache is a fresh directory, never bench/data."""

    def connect(self: socket.socket, address: Any) -> None:
        # an OSError, as a refused connection is: the callers close the socket
        raise ConnectionRefusedError(f"a test tried to connect to {address}")

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect)
    monkeypatch.setenv("TPQ_BENCH_DATA", str(tmp_path_factory.mktemp("cache")))
