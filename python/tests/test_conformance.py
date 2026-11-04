"""The conformance vectors in testdata/: every file read to canonical JSON equals its
expected .jsonl byte for byte, its footer matches the manifest, and every payload vector
decodes (or fails) as listed."""

from __future__ import annotations

import json
from typing import Any

import pyarrow.parquet as pq
import pytest

import tparquet as ppc
from conftest import TESTDATA, manifest, vector
from tparquet import canonical, codec

FILES = manifest()
PAYLOADS: list[dict[str, Any]] = json.loads((TESTDATA / "payloads" / "manifest.json").read_text())["vectors"]


def test_every_vector_is_covered() -> None:
    names = {f["name"] for f in FILES}
    assert names == {p.stem for p in (TESTDATA / "files").glob("*.parquet")}
    assert "standard_control" in names
    assert len(PAYLOADS) == len(list((TESTDATA / "payloads").glob("*.bin")))


@pytest.mark.parametrize("entry", FILES, ids=[f["name"] for f in FILES])
def test_file_reads_to_the_expected_rows(entry: dict[str, Any]) -> None:
    expected = (TESTDATA / entry["expected"]).read_bytes()
    table = ppc.read_table(TESTDATA / entry["file"])
    assert canonical.dumps(table).encode() == expected


@pytest.mark.parametrize("entry", FILES, ids=[f["name"] for f in FILES])
def test_file_streams_to_the_expected_rows(entry: dict[str, Any]) -> None:
    # small batches, through the C stream: batch boundaries do not change the rows
    expected = (TESTDATA / entry["expected"]).read_bytes()
    out = "".join(canonical.dumps(b) for b in ppc.ParquetFile(TESTDATA / entry["file"]).iter_batches(batch_size=97))
    assert out.encode() == expected
    assert canonical.dumps(ppc.read_batches(TESTDATA / entry["file"], batch_size=1000)).encode() == expected


@pytest.mark.parametrize("entry", FILES, ids=[f["name"] for f in FILES])
def test_footer_matches_the_manifest(entry: dict[str, Any]) -> None:
    path = TESTDATA / entry["file"]
    md = ppc.ParquetFile(path).metadata
    assert md.num_rows == entry["rows"]
    assert md.num_row_groups == entry["row_groups"]
    assert md.pcodec_version == entry["pcodec_version"]
    assert md.codecs == {c["path"]: c["codecs"] for c in entry["columns"]}
    assert md.physical_types == {c["path"]: c["physical_type"] for c in entry["columns"]}
    report = ppc.inspect(path)
    assert report["rows"] == entry["rows"]
    assert report["pcodec_version"] == entry["pcodec_version"]
    for got, want in zip(report["columns"], entry["columns"], strict=True):
        assert {k: got[k] for k in want} == want


def test_standard_control_through_pyarrow() -> None:
    # the canonical writer agrees with the expected rows on a file pyarrow reads itself
    expected = (TESTDATA / "expected" / "standard_control.jsonl").read_bytes()
    assert canonical.dumps(pq.read_table(vector("standard_control"))).encode() == expected


VALID = [p for p in PAYLOADS if p["valid"]]
INVALID = [p for p in PAYLOADS if not p["valid"]]


@pytest.mark.parametrize("v", VALID, ids=[p["name"] for p in VALID])
def test_valid_payload(v: dict[str, Any]) -> None:
    payload = (TESTDATA / "payloads" / v["payload"]).read_bytes()
    decoded = (TESTDATA / "payloads" / v["decoded"]).read_bytes()
    assert len(decoded) == v["expected_len"]
    assert codec.decompress(payload, v["expected_len"]) == decoded
    assert codec.mode(payload) == v["mode"]
    # the reference encoder (which this package wraps) writes the vector exactly
    assert codec.compress(decoded, hint=v["hint"], level=v["level"]) == payload


@pytest.mark.parametrize("v", INVALID, ids=[p["name"] for p in INVALID])
def test_invalid_payload(v: dict[str, Any]) -> None:
    payload = (TESTDATA / "payloads" / v["payload"]).read_bytes()
    with pytest.raises(ppc.PcodecError) as e:
        codec.decompress(payload, v["expected_len"])
    assert str(e.value) == v["error"]
