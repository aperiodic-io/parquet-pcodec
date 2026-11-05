package io.aperiodic.tparquet.trino;

import io.aperiodic.tparquet.TparquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Generated trades, written by java/tparquet's writer: the rows of id {@code [from, to)} are a
 * function of the id, with increasing timestamps, so that every copy has the same rows and the
 * row groups' timestamp statistics are disjoint.
 */
final class Trades
{
    static final MessageType SCHEMA = MessageTypeParser.parseMessageType("""
            message trades {
              required int64 ts (TIMESTAMP(MILLIS,true));
              required int64 id;
              required double price;
              required double qty;
              required int32 size;
              required binary symbol (STRING);
              optional double bid;
            }""");
    /** The Hive table's columns for {@link #SCHEMA}. */
    static final String COLUMNS = "ts timestamp(9), id bigint, price double, qty double, size integer, symbol varchar, bid double";
    static final long T0 = 1_735_689_600_000L;
    static final long STEP_MILLIS = 100;
    static final String[] SYMBOLS = {"AAPL", "AMZN", "GOOG", "META", "MSFT", "NFLX", "NVDA", "TSLA"};

    private Trades() {}

    static long ts(long id)
    {
        return T0 + id * STEP_MILLIS;
    }

    /** A pseudo-random double in [0, 1000) of the id (splitmix64): a column no codec compresses much. */
    static double noise(long id)
    {
        long z = id * 0x9E3779B97F4A7C15L + 0x632BE59BD9B4E019L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        z ^= z >>> 31;
        return (z >>> 11) * 0x1.0p-53 * 1000;
    }

    static void write(Path file, long from, long to, CompressionCodecName codec)
            throws IOException
    {
        SimpleGroupFactory rows = new SimpleGroupFactory(SCHEMA);
        // parquet-java closes a row group at a compressed size: with the noise column, some 20,000 rows
        TparquetWriter.Options options = TparquetWriter.Options.defaults().withCodec(codec)
                .withRowGroupSize(256 << 10).withPageSize(32 << 10);
        try (ParquetWriter<Group> w = TparquetWriter.groups(file, SCHEMA, options)) {
            for (long id = from; id < to; id++) {
                double price = 1000 + ((id * 7) % 2000) / 100.0;
                Group g = rows.newGroup()
                        .append("ts", ts(id))
                        .append("id", id)
                        .append("price", price)
                        .append("qty", noise(id))
                        .append("size", (int) (id % 97) + 1)
                        .append("symbol", SYMBOLS[(int) (id % SYMBOLS.length)]);
                if (id % 10 != 0) {
                    g.append("bid", price - 0.01);
                }
                w.write(g);
            }
        }
    }

    static final MessageType SYMBOLS_SCHEMA = MessageTypeParser.parseMessageType("""
            message symbols {
              required binary symbol (STRING);
              required binary sector (STRING);
            }""");

    /** A standard (ZSTD) table of the symbols and their sectors. */
    static void writeSymbols(Path file)
            throws IOException
    {
        SimpleGroupFactory rows = new SimpleGroupFactory(SYMBOLS_SCHEMA);
        String[] sectors = {"tech", "retail", "tech", "media", "tech", "media", "chips", "auto"};
        try (ParquetWriter<Group> w = TparquetWriter.groups(file, SYMBOLS_SCHEMA, TparquetWriter.Options.defaults().withCodec(CompressionCodecName.ZSTD))) {
            for (int i = 0; i < SYMBOLS.length; i++) {
                w.write(rows.newGroup().append("symbol", SYMBOLS[i]).append("sector", sectors[i]));
            }
        }
    }
}
