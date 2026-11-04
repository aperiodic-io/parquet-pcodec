"""Property tests: random pyarrow tables round-trip bit for bit under every policy."""

from __future__ import annotations

import decimal
import io
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from hypothesis import given
from hypothesis import strategies as st

import pparquet as ppc
from pparquet import canonical


def ints(bits: int, signed: bool = True) -> st.SearchStrategy[int]:
    if signed:
        return st.integers(-(2 ** (bits - 1)), 2 ** (bits - 1) - 1)
    return st.integers(0, 2**bits - 1)


@st.composite
def numbers(draw: st.DrawFn, n: int, bits: int, signed: bool = True) -> list[int]:
    """Integers: arbitrary, or smooth (a random walk), or a few repeated values."""
    kind = draw(st.sampled_from(["any", "walk", "few"]))
    lo, hi = (-(2 ** (bits - 1)), 2 ** (bits - 1) - 1) if signed else (0, 2**bits - 1)
    if kind == "any":
        return draw(st.lists(ints(bits, signed), min_size=n, max_size=n))
    if kind == "few":
        pool = draw(st.lists(ints(bits, signed), min_size=1, max_size=4))
        return [pool[i % len(pool)] for i in draw(st.lists(st.integers(0, 3), min_size=n, max_size=n))]
    start = draw(ints(bits, signed))
    steps = draw(st.lists(st.integers(-5, 5), min_size=n, max_size=n))
    out, v = [], start
    for s in steps:
        v = min(hi, max(lo, v + s))
        out.append(v)
    return out


@st.composite
def float_bits(draw: st.DrawFn, n: int, bits: int) -> pa.Array[Any]:
    """Floats from arbitrary bit patterns (NaN payloads, -0.0, subnormals) or decimal prices."""
    if draw(st.booleans()):
        raw = draw(st.lists(ints(bits, signed=False), min_size=n, max_size=n))
        unsigned = pa.uint64() if bits == 64 else pa.uint32()
        return pa.array(raw, unsigned).view(pa.float64() if bits == 64 else pa.float32())
    cents = draw(numbers(n, 32))
    return pa.array([c / 100 for c in cents], pa.float64() if bits == 64 else pa.float32())


def nulled(draw: st.DrawFn, arr: pa.Array[Any]) -> pa.Array[Any]:
    """`arr`, or `arr` with random elements made null."""
    if len(arr) == 0 or not draw(st.booleans()):
        return arr
    mask = draw(st.lists(st.booleans(), min_size=len(arr), max_size=len(arr)))
    out: pa.Array[Any] = pc.if_else(pa.array(mask), pa.nulls(len(arr), arr.type), arr)
    return out


COLUMN_KINDS = [
    "i8",
    "i16",
    "i32",
    "i64",
    "u32",
    "u64",
    "f32",
    "f64",
    "bool",
    "str",
    "ts_ms_tz",
    "date32",
    "dec64",
    "list_f64",
    "list_i32",
    "struct",
]


@st.composite
def column(draw: st.DrawFn, kind: str, n: int) -> pa.Array[Any]:
    simple = {
        "i8": (8, True, pa.int8()),
        "i16": (16, True, pa.int16()),
        "i32": (32, True, pa.int32()),
        "i64": (64, True, pa.int64()),
        "u32": (32, False, pa.uint32()),
        "u64": (64, False, pa.uint64()),
    }
    if kind in simple:
        bits, signed, t = simple[kind]
        return nulled(draw, pa.array(draw(numbers(n, bits, signed)), t))
    if kind in ("f32", "f64"):
        return nulled(draw, draw(float_bits(n, 64 if kind == "f64" else 32)))
    if kind == "bool":
        return nulled(draw, pa.array(draw(st.lists(st.booleans(), min_size=n, max_size=n))))
    if kind == "str":
        return nulled(draw, pa.array(draw(st.lists(st.text(max_size=5), min_size=n, max_size=n)), pa.string()))
    if kind == "ts_ms_tz":
        v = [1_735_689_600_000 + x for x in draw(numbers(n, 32))]
        return nulled(draw, pa.array(v, pa.timestamp("ms", tz="Asia/Tokyo")))
    if kind == "date32":
        return nulled(draw, pa.array([abs(x) % 100_000 for x in draw(numbers(n, 32))], pa.date32()))
    if kind == "dec64":
        decimals = [decimal.Decimal(x // 1000).scaleb(-4) for x in draw(numbers(n, 64))]
        return nulled(draw, pa.array(decimals, pa.decimal128(18, 4)))
    if kind in ("list_f64", "list_i32"):
        lengths = draw(st.lists(st.integers(0, 4), min_size=n, max_size=n))
        total = sum(lengths)
        values: pa.Array[Any] = (
            draw(float_bits(total, 64)) if kind == "list_f64" else pa.array(draw(numbers(total, 32)), pa.int32())
        )
        offsets = [0]
        for length in lengths:
            offsets.append(offsets[-1] + length)
        return nulled(draw, pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), values))
    a = pa.array(draw(numbers(n, 64)), pa.int64())
    b = draw(float_bits(n, 64))
    return nulled(draw, pa.StructArray.from_arrays([a, b], ["a", "b"]))


@st.composite
def tables(draw: st.DrawFn) -> pa.Table:
    n = draw(st.integers(0, 120))
    kinds = draw(st.lists(st.sampled_from(COLUMN_KINDS), min_size=1, max_size=4))
    return pa.table({f"{k}_{i}": draw(column(k, n)) for i, k in enumerate(kinds)})


@given(table=tables(), policy=st.sampled_from(["auto", "pcodec", "base"]), rows=st.integers(1, 64))
def test_tables_round_trip_bit_for_bit(table: pa.Table, policy: ppc.Policy, rows: int) -> None:
    buf = io.BytesIO()
    ppc.write_table(table, buf, policy=policy, row_group_size=rows)
    back = ppc.read_table(io.BytesIO(buf.getvalue()))
    assert back.schema == table.schema
    assert canonical.dumps(back) == canonical.dumps(table)


@given(table=tables(), level=st.integers(0, 12), chunk=st.integers(1, 50))
def test_streamed_batches_round_trip(table: pa.Table, level: int, chunk: int) -> None:
    buf = io.BytesIO()
    with ppc.Writer(buf, table.schema, policy="pcodec", level=level) as w:
        for b in table.to_batches(max_chunksize=chunk):
            w.write(b)
    back = ppc.read_table(io.BytesIO(buf.getvalue()), batch_size=chunk)
    assert canonical.dumps(back) == canonical.dumps(table)


@given(data=st.binary(max_size=2000), hint=st.sampled_from([None, "INT32", "INT64", "FLOAT", "DOUBLE"]))
def test_payloads_round_trip(data: bytes, hint: Any) -> None:
    from pparquet import codec

    assert codec.decompress(codec.compress(data, hint=hint), len(data)) == data


@given(payload=st.binary(max_size=300), expected=st.integers(0, 5000))
def test_garbage_payloads_raise_cleanly(payload: bytes, expected: int) -> None:
    from pparquet import codec

    try:
        out = codec.decompress(payload, expected)
    except ppc.PcodecError:
        return
    assert len(out) == expected
