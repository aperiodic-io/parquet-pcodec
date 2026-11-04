"""parquet_pcodec.codec: the PCODEC page payload on its own."""

from __future__ import annotations

import random
import struct

import pytest

import parquet_pcodec as ppc
from parquet_pcodec import codec


def test_round_trip_each_hint() -> None:
    ints = struct.pack("<1000q", *range(0, 10_000, 10))
    hints: list[codec.Hint | None] = ["INT32", "INT64", "FLOAT", "DOUBLE", None]
    for hint in hints:
        payload = codec.compress(ints, hint=hint)
        assert codec.decompress(payload, len(ints)) == ints
    assert codec.mode(codec.compress(ints, hint="INT64")) == "PCO"
    assert codec.mode(codec.compress(ints)) == "ZSTD"
    assert codec.mode(codec.compress(b"")) == "RAW"
    prices = struct.pack("<500d", *(100 + i / 100 for i in range(500)))
    assert codec.mode(codec.compress(prices, hint="DOUBLE")) == "DECIMAL"
    assert codec.decompress(codec.compress(prices, hint="DOUBLE", level=12), len(prices)) == prices


def test_level_changes_the_payload() -> None:
    r = random.Random(1)
    data = struct.pack("<4000q", *(int(r.gauss(0, 1e6)) for _ in range(4000)))
    assert codec.compress(data, hint="INT64", level=0) != codec.compress(data, hint="INT64", level=12)
    assert codec.compress(data, hint="INT64") == codec.compress(data, hint="INT64", level=8)


def test_constants() -> None:
    assert codec.CODEC_ID == 20547
    assert codec.FORMAT_VERSION == 1
    assert codec.KV_KEY == "parquet.pcodec.version"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"hint": "BOOLEAN"}, "hint"),
        ({"level": 13}, "level 13"),
        ({"level": -1}, "level -1"),
    ],
)
def test_bad_options(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ppc.PcodecError, match=message):
        codec.compress(b"12345678", **kwargs)  # type: ignore[arg-type]


def test_bad_payloads() -> None:
    with pytest.raises(ppc.PcodecError, match="truncated"):
        codec.mode(b"\x01")
    with pytest.raises(ppc.PcodecError, match="unknown mode 9"):
        codec.mode(b"\x01\x09")
    with pytest.raises(ppc.PcodecError, match="expected_len"):
        codec.decompress(b"\x01\x00", -1)
