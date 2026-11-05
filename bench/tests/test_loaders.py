"""Loaders: provider files as Arrow record batches with the provider's documented types."""

from __future__ import annotations

import datetime as dt
import gzip
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tpq_bench import loaders


def gz(path: pathlib.Path, text: str) -> pathlib.Path:
    path.write_bytes(gzip.compress(text.encode()))
    return path


def test_tardis_trades_get_tardis_types(tmp_path: pathlib.Path) -> None:
    f = gz(
        tmp_path / "trades.csv.gz",
        "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
        "binance-futures,BTCUSDT,1748736000018000,1748736000020845,6360532016,sell,104544.6,0.01\n"
        "binance-futures,BTCUSDT,1748736000019000,1748736000021000,6360532017,buy,104544.7,1.5\n",
    )
    table = loaders.read_all(loaders.load("tardis", f))
    utc = pa.timestamp("us", tz="UTC")
    expected: list[tuple[str, pa.DataType]] = [
        ("exchange", pa.string()),
        ("symbol", pa.string()),
        ("timestamp", utc),
        ("local_timestamp", utc),
        ("id", pa.string()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("amount", pa.float64()),
    ]
    assert table.schema == pa.schema(expected)
    assert table.column("timestamp")[0].value == 1748736000018000
    assert table.column("id").to_pylist() == ["6360532016", "6360532017"]
    assert table.column("price").to_pylist() == [104544.6, 104544.7]


def test_tardis_empty_numbers_are_null(tmp_path: pathlib.Path) -> None:
    f = gz(
        tmp_path / "derivative_ticker.csv.gz",
        "exchange,symbol,timestamp,local_timestamp,funding_timestamp,funding_rate,open_interest\n"
        "binance-futures,BTCUSDT,1748736000002000,1748736000039567,1748736000000000,-0.00000582,\n"
        "binance-futures,BTCUSDT,1748736000003000,1748736000040000,,,81000.5\n",
    )
    table = loaders.read_all(loaders.load("tardis", f))
    assert table.schema.field("funding_timestamp").type == pa.timestamp("us", tz="UTC")
    assert table.column("funding_timestamp").null_count == 1
    assert table.column("funding_rate").to_pylist() == [-0.00000582, None]
    assert table.column("open_interest").to_pylist() == [None, 81000.5]


def test_tardis_option_expirations_are_timestamps(tmp_path: pathlib.Path) -> None:
    f = gz(
        tmp_path / "options_chain.csv.gz",
        "exchange,symbol,timestamp,local_timestamp,type,strike_price,expiration\n"
        "deribit,BTC-27JUN25-100000-C,1,2,call,100000,1751011200000000\n",
    )
    table = loaders.read_all(loaders.load("tardis", f))
    assert table.schema.field("expiration").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("type").type == pa.string()
    assert table.column("expiration")[0].value == 1751011200000000


def test_tardis_book_columns_keep_their_names(tmp_path: pathlib.Path) -> None:
    f = gz(
        tmp_path / "book.csv.gz",
        "exchange,symbol,timestamp,local_timestamp,asks[0].price,asks[0].amount,bids[0].price,bids[0].amount\n"
        "deribit,BTC-PERPETUAL,1,2,10.5,3,10,4\n",
    )
    table = loaders.read_all(loaders.load("tardis", f))
    assert table.schema.names[4:] == ["asks[0].price", "asks[0].amount", "bids[0].price", "bids[0].amount"]
    assert {table.schema.field(n).type for n in table.schema.names[4:]} == {pa.float64()}


# NYSE TAQ client files: one CSV line per message, the first field is the message type
NYSE = (
    "3,2,MSFT,1,12,Q,C,100,80.3,0,0,N,.0001,1\n"
    "34,4,00:28:12.757464543,MSFT,1,P,~,,,,,,~,P\n"
    "220,9348,07:00:00.131306712,MSFT,4,103,378.61,2,@, ,T,I\n"
    "220,9349,15:59:59.000000001,JEM,5,104,8.2,200,@,F,T, \n"
    "140,980,09:30:00.733235874,ELME,3,1.51,900,1.46,1000,O, \n"
    "140,981,09:30:01.000000000,ELME,4,0,0,1.45,,R, \n"
)


def test_nyse_trades_keep_the_trade_messages(tmp_path: pathlib.Path) -> None:
    f = gz(tmp_path / "EQY_US_TAQ_NYSE_1_TRADES_20260701.gz", NYSE)
    table = loaders.read_all(loaders.load("nyse_trades", f))
    ny = pa.timestamp("ns", tz="America/New_York")
    expected: list[tuple[str, pa.DataType]] = [
        ("seq", pa.int64()),
        ("time", ny),
        ("symbol", pa.string()),
        ("symbol_seq", pa.int64()),
        ("trade_id", pa.int64()),
        ("price", pa.float64()),
        ("volume", pa.int64()),
        ("cond1", pa.string()),
        ("cond2", pa.string()),
        ("cond3", pa.string()),
        ("cond4", pa.string()),
    ]
    assert table.schema == pa.schema(expected)
    assert table.column("seq").to_pylist() == [9348, 9349]
    assert table.column("price").to_pylist() == [378.61, 8.2]
    assert table.column("cond2").to_pylist() == [" ", "F"]
    # 07:00 New York on 2026-07-01 is 11:00 UTC (EDT), to the nanosecond
    first = dt.datetime(2026, 7, 1, 11, 0, 0, tzinfo=dt.timezone.utc)
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
    assert table.column("time")[0].value == (first - epoch) // dt.timedelta(microseconds=1) * 1000 + 131306712
    assert (
        table.column("time")[1].value - table.column("time")[0].value == ((8 * 60 + 59) * 60 + 59) * 10**9 - 131306711
    )


def test_nyse_bbo_keeps_the_quote_messages(tmp_path: pathlib.Path) -> None:
    f = gz(tmp_path / "EQY_US_TAQ_NYSE_1_BBO_20260701.gz", NYSE)
    table = loaders.read_all(loaders.load("nyse_bbo", f))
    assert table.schema.names == [
        "seq",
        "time",
        "symbol",
        "symbol_seq",
        "ask_price",
        "ask_volume",
        "bid_price",
        "bid_volume",
        "quote_condition",
        "rpi_indicator",
    ]
    assert table.column("ask_price").to_pylist() == [1.51, 0.0]
    assert table.column("bid_volume").to_pylist() == [1000, None]
    assert table.column("quote_condition").to_pylist() == ["O", "R"]


def test_nyse_date_comes_from_the_file_name(tmp_path: pathlib.Path) -> None:
    f = gz(tmp_path / "trades.gz", NYSE)
    with pytest.raises(ValueError, match="date"):
        loaders.load("nyse_trades", f)


def test_nyse_refuses_a_day_with_a_clock_change(tmp_path: pathlib.Path) -> None:
    f = gz(tmp_path / "EQY_US_TAQ_NYSE_1_TRADES_20260308.gz", NYSE)
    with pytest.raises(ValueError, match="clock"):
        loaders.read_all(loaders.load("nyse_trades", f))


def test_nyse_takes_the_day_before_a_clock_change(tmp_path: pathlib.Path) -> None:
    f = gz(tmp_path / "EQY_US_TAQ_NYSE_1_TRADES_20260307.gz", NYSE)
    table = loaders.read_all(loaders.load("nyse_trades", f))
    # 07:00 New York on 2026-03-07 is 12:00 UTC (EST)
    noon = dt.datetime(2026, 3, 7, 12, tzinfo=dt.timezone.utc)
    assert table.column("time")[0].value == int(noon.timestamp()) * 10**9 + 131306712


def test_parquet_is_read_as_delivered(tmp_path: pathlib.Path) -> None:
    table = pa.table({"t": pa.array([1, 2, 3], pa.int64()), "side": ["a", "b", "a"]})
    pq.write_table(table, tmp_path / "x.parquet", compression="snappy")
    assert loaders.read_all(loaders.load("parquet", tmp_path / "x.parquet")) == table


def test_unknown_loader() -> None:
    with pytest.raises(ValueError, match="loader"):
        loaders.load("xml", pathlib.Path("x"))


def dbn_trades(path: pathlib.Path) -> pathlib.Path:
    import databento_dbn as d

    meta = d.Metadata(
        dataset="GLBX.MDP3",
        schema=d.Schema.TRADES,  # type: ignore[arg-type]  # the stubs type the enum members as str
        start=1_700_000_000_000_000_000,
        end=1_700_000_100_000_000_000,
        stype_in=d.SType.RAW_SYMBOL,  # type: ignore[arg-type]  # the stubs type the enum members as str
        stype_out=d.SType.INSTRUMENT_ID,  # type: ignore[arg-type]  # the stubs type the enum members as str
        symbols=["ESZ3"],
        partial=[],
        not_found=[],
        mappings=[],
    )
    records = [
        d.TradeMsg(
            publisher_id=1,
            instrument_id=42,
            ts_event=1_700_000_000_000_000_000 + i,
            price=4_500_250_000_000 + i * 250_000_000,
            size=1 + i,
            action=d.Action.TRADE,  # type: ignore[arg-type]  # the stubs type the enum members as str
            side=d.Side.ASK,  # type: ignore[arg-type]  # the stubs type the enum members as str
            depth=0,
            ts_recv=1_700_000_000_000_001_000 + i,
            flags=0,
            ts_in_delta=10,
            sequence=i,
        )
        for i in range(3)
    ]
    path.write_bytes(meta.encode() + b"".join(bytes(r) for r in records))
    return path


def test_dbn_is_read_as_the_client_converts_it(tmp_path: pathlib.Path) -> None:
    pytest.importorskip("databento")
    f = dbn_trades(tmp_path / "GLBX.MDP3_ESZ3_trades.dbn")
    table = loaders.read_all(loaders.load("dbn", f))
    assert table.column("price").to_pylist() == [4500.25, 4500.5, 4500.75]
    assert table.schema.field("ts_recv").type == pa.timestamp("ns", tz="UTC")
    assert table.column("size").to_pylist() == [1, 2, 3]
    # the conversion is cached next to the DBN file
    assert (tmp_path / "GLBX.MDP3_ESZ3_trades.dbn.parquet").exists()
    assert loaders.read_all(loaders.load("dbn", f)) == table


def test_dbn_schema(tmp_path: pathlib.Path) -> None:
    pytest.importorskip("databento")
    assert loaders.dbn_schema(dbn_trades(tmp_path / "x.dbn")) == "trades"
