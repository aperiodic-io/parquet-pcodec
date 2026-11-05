"""Provider files as streams of Arrow record batches, typed as each provider documents them.

Parquet files (Aperiodic, Crypto Lake) are read as delivered. The CSV files get the types
their documentation gives: Tardis.dev (https://docs.tardis.dev/downloadable-csv-files)
and the NYSE TAQ client specifications. DBN files are converted by Databento's own client
(`DBNStore.to_parquet`: prices as floats, timestamps as timestamps).
"""

from __future__ import annotations

import contextlib
import datetime as dt
import pathlib
import re
import warnings
import zoneinfo
from collections.abc import Callable, Iterator
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq

BLOCK = 16 << 20

Loader = Callable[[pathlib.Path], pa.RecordBatchReader]


def read_all(reader: pa.RecordBatchReader) -> pa.Table:
    return reader.read_all()


def _reader(schema: pa.Schema, batches: Iterator[pa.RecordBatch]) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_batches(schema, batches)


def parquet(path: pathlib.Path) -> pa.RecordBatchReader:
    f = pq.ParquetFile(path)
    # any batch size reads the same rows: a memory and speed knob
    return _reader(f.schema_arrow, f.iter_batches(batch_size=1 << 16))  # pragma: no mutate


def _number(column: pa.Array[Any], type: pa.DataType) -> pa.Array[Any]:
    """A CSV string column as `type`, empty fields as nulls."""
    empty = pc.equal(column, pa.scalar(""))
    # (the stubs make a cast of an array a scalar; typing.cast does nothing at run time)
    return cast("pa.Array[Any]", pc.cast(pc.if_else(empty, None, column), type))  # pragma: no mutate


# --- Tardis.dev: these columns are strings, `*timestamp` columns µs since the epoch (UTC), and
# every other column a number
_TARDIS_STRINGS = {"exchange", "symbol", "id", "side", "type", "option_type"}


def _tardis_type(name: str) -> pa.DataType:
    if name in _TARDIS_STRINGS:
        return pa.string()
    if name.endswith("timestamp") or name == "expiration":
        return pa.timestamp("us", tz="UTC")
    return pa.float64()


def tardis(path: pathlib.Path) -> pa.RecordBatchReader:
    # every column read as text: empty fields become nulls of the column's type below
    with pa.input_stream(str(path)) as head:  # the compression is detected from the extension
        # the header line, within the first MB (a 25-level book's is 2 KB)
        names = head.read(1 << 20).partition(b"\n")[0].decode().strip().split(",")  # pragma: no mutate
    stream = csv.open_csv(
        path,
        read_options=csv.ReadOptions(block_size=BLOCK),  # pragma: no mutate (batch size: speed, memory)
        convert_options=csv.ConvertOptions(column_types={n: pa.string() for n in names}),
    )
    schema = pa.schema([(n, _tardis_type(n)) for n in names])

    def batches() -> Iterator[pa.RecordBatch]:
        for batch in stream:
            columns = []
            for name, field in zip(names, schema):
                column = batch.column(name)
                if field.type == pa.timestamp("us", tz="UTC"):
                    column = pc.cast(_number(column, pa.int64()), field.type)
                elif field.type != pa.string():
                    column = _number(column, field.type)
                columns.append(column)
            yield pa.record_batch(columns, schema=schema)

    return _reader(schema, batches())


# --- NYSE TAQ client files: one CSV line per message, its type first; times are New York
# wall-clock times (HH:MM:SS.nnnnnnnnn) of the day in the file name
NEW_YORK = "America/New_York"
TIME = pa.timestamp("ns", tz=NEW_YORK)
Fields = tuple[tuple[str, "pa.DataType | None"], ...]
# the message type is dropped: one per file
_NYSE_TRADES: Fields = (
    ("msg_type", None),
    ("seq", pa.int64()),
    ("time", TIME),
    ("symbol", pa.string()),
    ("symbol_seq", pa.int64()),
    ("trade_id", pa.int64()),
    ("price", pa.float64()),
    ("volume", pa.int64()),
    ("cond1", pa.string()),
    ("cond2", pa.string()),
    ("cond3", pa.string()),
    ("cond4", pa.string()),
)
_NYSE_BBO: Fields = (
    ("msg_type", None),
    ("seq", pa.int64()),
    ("time", TIME),
    ("symbol", pa.string()),
    ("symbol_seq", pa.int64()),
    ("ask_price", pa.float64()),
    ("ask_volume", pa.int64()),
    ("bid_price", pa.float64()),
    ("bid_volume", pa.int64()),
    ("quote_condition", pa.string()),
    ("rpi_indicator", pa.string()),
)


def _midnight_ns(day: dt.date) -> int:
    """New York midnight of `day`, ns since the epoch; refuses a day whose UTC offset changes."""
    zone = zoneinfo.ZoneInfo(NEW_YORK)
    start = dt.datetime.combine(day, dt.time(0), zone)
    end = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(0), zone)
    if start.utcoffset() != end.utcoffset():
        raise ValueError(f"{day}: New York's clock changes that day; the loader assumes one UTC offset")
    return int(start.timestamp()) * 10**9


def _nyse_time(column: pa.Array[Any], midnight: int) -> pa.Array[Any]:
    def part(start: int, stop: int) -> pa.Array[Any]:
        return pc.cast(pc.utf8_slice_codeunits(column, start, stop), pa.int64())

    seconds = pc.add(pc.multiply(pc.add(pc.multiply(part(0, 2), 60), part(3, 5)), 60), part(6, 8))
    # the nine digits after the point (any stop past them is the same)
    ns = pc.add(pc.add(pc.multiply(seconds, 10**9), part(9, 18)), midnight)  # pragma: no mutate
    return pc.cast(ns, TIME)


def _nyse(path: pathlib.Path, message: str, fields: Fields) -> pa.RecordBatchReader:
    found = re.search(r"_(\d{8})\.gz$", path.name)
    if found is None:
        raise ValueError(f"{path.name}: no date (YYYYMMDD.gz) in the file name")
    day = dt.datetime.strptime(found.group(1), "%Y%m%d").date()
    names = [n for n, _ in fields]
    stream = csv.open_csv(
        path,
        read_options=csv.ReadOptions(column_names=names, block_size=BLOCK),  # pragma: no mutate (speed, memory)
        # lines of the other messages have another number of fields
        parse_options=csv.ParseOptions(invalid_row_handler=lambda row: "skip"),
        convert_options=csv.ConvertOptions(column_types={n: pa.string() for n in names}),
    )
    schema = pa.schema([pa.field(n, t) for n, t in fields if t is not None])

    def batches() -> Iterator[pa.RecordBatch]:
        midnight = _midnight_ns(day)
        for batch in stream:
            batch = batch.filter(pc.equal(batch.column("msg_type"), pa.scalar(message)))
            columns = []
            for field in schema:
                column = batch.column(field.name)
                if field.name == "time":
                    column = _nyse_time(column, midnight)
                elif field.type != pa.string():
                    column = _number(column, field.type)
                columns.append(column)
            yield pa.record_batch(columns, schema=schema)

    return _reader(schema, batches())


def nyse_trades(path: pathlib.Path) -> pa.RecordBatchReader:
    return _nyse(path, "220", _NYSE_TRADES)


def nyse_bbo(path: pathlib.Path) -> pa.RecordBatchReader:
    return _nyse(path, "140", _NYSE_BBO)


# --- Databento DBN


@contextlib.contextmanager
def _store(path: pathlib.Path) -> Iterator[Any]:
    import databento

    # an open file of our own: the client's `from_file` leaves its file to the garbage collector
    with open(path, "rb") as f:
        yield databento.DBNStore.from_bytes(f)


def dbn_schema(path: pathlib.Path) -> str:
    with _store(path) as store:
        return str(store.schema)


def dbn(path: pathlib.Path) -> pa.RecordBatchReader:
    converted = path.with_name(path.name + ".parquet")
    if not converted.exists():
        part = converted.with_name(converted.name + ".part")  # pragma: no mutate (any temporary name)
        with warnings.catch_warnings():
            # the client's own use of pandas warns on recent pandas
            warnings.simplefilter("ignore")
            with _store(path) as store:
                store.to_parquet(part)
        part.replace(converted)
    return parquet(converted)


LOADERS: dict[str, Loader] = {
    "parquet": parquet,
    "tardis": tardis,
    "nyse_trades": nyse_trades,
    "nyse_bbo": nyse_bbo,
    "dbn": dbn,
}


def load(loader: str, path: pathlib.Path) -> pa.RecordBatchReader:
    if loader not in LOADERS:
        raise ValueError(f"unknown loader {loader!r}")
    return LOADERS[loader](path)
