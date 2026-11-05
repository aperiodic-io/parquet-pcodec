/*
 * tparquet fork (trino/trino-parquet/FORK.md): new file.
 */
package io.trino.parquet.writer;

import io.aperiodic.tparquet.Mode;
import io.aperiodic.tparquet.Pcodec;
import io.aperiodic.tparquet.PhysicalType;
import io.airlift.slice.DynamicSliceOutput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.trino.parquet.writer.ParquetCompressor.getCompressor;
import static io.trino.parquet.writer.PcodecColumns.hint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The tparquet fork: ParquetCompressor for PCODEC, with the column's physical type as the hint. */
class TestPcodecCompressor
{
    private static ColumnDescriptor column(PrimitiveTypeName type, Repetition repetition)
    {
        PrimitiveType p = Types.primitive(type, repetition).length(type == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ? 16 : 0).named("c");
        int definition = repetition == Repetition.REQUIRED ? 0 : 1;
        int repeat = repetition == Repetition.REPEATED ? 1 : 0;
        return new ColumnDescriptor(new String[] {"c"}, p, repeat, definition);
    }

    private static byte[] bytes(ParquetDataOutput out)
    {
        DynamicSliceOutput sink = new DynamicSliceOutput(out.size());
        out.writeData(sink);
        return sink.slice().getBytes();
    }

    private static ByteBuffer longs(int n)
    {
        ByteBuffer b = ByteBuffer.allocate(n * 8).order(ByteOrder.LITTLE_ENDIAN);
        long v = 1_735_689_600_000L;
        for (int i = 0; i < n; i++) {
            v += 1000 + (i % 7);
            b.putLong(v);
        }
        return b.flip();
    }

    @Test
    void theHintIsThePhysicalTypeOfARequiredFlatNumericColumn()
    {
        assertThat(hint(column(PrimitiveTypeName.INT32, Repetition.REQUIRED))).isSameAs(PhysicalType.INT32);
        assertThat(hint(column(PrimitiveTypeName.INT64, Repetition.REQUIRED))).isSameAs(PhysicalType.INT64);
        assertThat(hint(column(PrimitiveTypeName.FLOAT, Repetition.REQUIRED))).isSameAs(PhysicalType.FLOAT);
        assertThat(hint(column(PrimitiveTypeName.DOUBLE, Repetition.REQUIRED))).isSameAs(PhysicalType.DOUBLE);
    }

    @Test
    void noHintWhereThePageIsNotAnArrayOfNumbers()
    {
        // V1 pages of optional or repeated columns start with their levels (spec section 3)
        for (Repetition r : new Repetition[] {Repetition.OPTIONAL, Repetition.REPEATED}) {
            for (PrimitiveTypeName t : new PrimitiveTypeName[] {PrimitiveTypeName.INT32, PrimitiveTypeName.INT64, PrimitiveTypeName.FLOAT, PrimitiveTypeName.DOUBLE}) {
                assertThat(hint(column(t, r))).as("%s %s", r, t).isNull();
            }
        }
        // types PCODEC does not model
        for (PrimitiveTypeName t : new PrimitiveTypeName[] {PrimitiveTypeName.BOOLEAN, PrimitiveTypeName.INT96, PrimitiveTypeName.BINARY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY}) {
            assertThat(hint(column(t, Repetition.REQUIRED))).as("%s", t).isNull();
        }
        // a required leaf inside an optional group has a definition level
        PrimitiveType leaf = Types.required(PrimitiveTypeName.INT64).named("c");
        assertThat(hint(new ColumnDescriptor(new String[] {"g", "c"}, leaf, 0, 1))).isNull();
    }

    @Test
    void compressesWithTheHint()
            throws IOException
    {
        ByteBuffer input = longs(10_000);
        byte[] expected = new byte[input.remaining()];
        input.duplicate().get(expected);
        byte[] payload = bytes(getCompressor(CompressionCodec.PCODEC, column(PrimitiveTypeName.INT64, Repetition.REQUIRED)).compress(input));
        assertThat(Pcodec.mode(payload)).isSameAs(Mode.PCO);
        assertThat(payload.length).isLessThan(expected.length / 8);
        assertThat(Pcodec.decompress(payload, expected.length)).isEqualTo(expected);
    }

    @Test
    void withoutAHintThePayloadIsTheZstdFallback()
            throws IOException
    {
        ByteBuffer input = longs(10_000);
        byte[] expected = new byte[input.remaining()];
        input.duplicate().get(expected);
        byte[] payload = bytes(getCompressor(CompressionCodec.PCODEC, column(PrimitiveTypeName.INT64, Repetition.OPTIONAL)).compress(input));
        assertThat(Pcodec.mode(payload)).isSameAs(Mode.ZSTD);
        assertThat(Pcodec.decompress(payload, expected.length)).isEqualTo(expected);
    }

    @Test
    void compressesTheRemainingBytesOfAnyBuffer()
            throws IOException
    {
        byte[] data = new byte[4099];
        new Random(7).nextBytes(data);
        ParquetCompressor compressor = getCompressor(CompressionCodec.PCODEC, column(PrimitiveTypeName.INT32, Repetition.REQUIRED));
        // a heap buffer at an offset, and a direct one
        ByteBuffer heap = ByteBuffer.wrap(data, 3, 4096).slice();
        ByteBuffer direct = ByteBuffer.allocateDirect(4096).put(ByteBuffer.wrap(data, 3, 4096)).flip();
        byte[] expected = java.util.Arrays.copyOfRange(data, 3, 4099);
        assertThat(Pcodec.decompress(bytes(compressor.compress(heap)), 4096)).isEqualTo(expected);
        assertThat(Pcodec.decompress(bytes(compressor.compress(direct)), 4096)).isEqualTo(expected);
        // an empty page
        assertThat(Pcodec.mode(bytes(compressor.compress(ByteBuffer.allocate(0))))).isSameAs(Mode.RAW);
    }

    @Test
    void theCompressorOfAColumnHasItsHint()
    {
        assertThat(getCompressor(CompressionCodec.PCODEC, column(PrimitiveTypeName.DOUBLE, Repetition.REQUIRED)))
                .isInstanceOfSatisfying(PcodecColumns.PcodecCompressor.class, c -> assertThat(c.hint()).isSameAs(PhysicalType.DOUBLE));
        assertThat(getCompressor(CompressionCodec.PCODEC, column(PrimitiveTypeName.BINARY, Repetition.REQUIRED)))
                .isInstanceOfSatisfying(PcodecColumns.PcodecCompressor.class, c -> assertThat(c.hint()).isNull());
    }

    @Test
    void plainValuesForTheModeledColumnsOfAPcodecFile()
    {
        assertThat(PcodecColumns.plainValues(CompressionCodec.PCODEC, column(PrimitiveTypeName.INT64, Repetition.REQUIRED))).isTrue();
        assertThat(PcodecColumns.plainValues(CompressionCodec.PCODEC, column(PrimitiveTypeName.INT64, Repetition.OPTIONAL))).isFalse();
        assertThat(PcodecColumns.plainValues(CompressionCodec.PCODEC, column(PrimitiveTypeName.BINARY, Repetition.REQUIRED))).isFalse();
        for (CompressionCodec codec : CompressionCodec.values()) {
            if (codec != CompressionCodec.PCODEC) {
                assertThat(PcodecColumns.plainValues(codec, column(PrimitiveTypeName.INT64, Repetition.REQUIRED))).as("%s", codec).isFalse();
            }
        }
    }

    private static org.apache.parquet.format.RowGroup rowGroup(CompressionCodec... codecs)
    {
        List<org.apache.parquet.format.ColumnChunk> chunks = new ArrayList<>();
        for (CompressionCodec codec : codecs) {
            org.apache.parquet.format.ColumnChunk chunk = new org.apache.parquet.format.ColumnChunk(0);
            chunk.setMeta_data(new ColumnMetaData(org.apache.parquet.format.Type.INT64, List.of(), List.of("c"), codec, 1, 8, 8, 4));
            chunks.add(chunk);
        }
        return new org.apache.parquet.format.RowGroup(chunks, 8, 1);
    }

    @Test
    void theFooterKeyMarksAFileWithAPcodecChunk()
    {
        KeyValue key = new KeyValue(Pcodec.KV_KEY).setValue(Pcodec.KV_VALUE);
        assertThat(PcodecColumns.footerKeyValue(List.of(rowGroup(CompressionCodec.ZSTD), rowGroup(CompressionCodec.SNAPPY, CompressionCodec.PCODEC)))).contains(key);
        assertThat(PcodecColumns.footerKeyValue(List.of(rowGroup(CompressionCodec.PCODEC)))).contains(key);
        assertThat(PcodecColumns.footerKeyValue(List.of(rowGroup(CompressionCodec.ZSTD, CompressionCodec.UNCOMPRESSED)))).isEmpty();
        assertThat(PcodecColumns.footerKeyValue(List.of())).isEmpty();
    }

    @Test
    void theOtherCodecsAreUnchanged()
    {
        ColumnDescriptor c = column(PrimitiveTypeName.INT64, Repetition.REQUIRED);
        assertThat(getCompressor(CompressionCodec.UNCOMPRESSED, c)).isNull();
        for (CompressionCodec codec : new CompressionCodec[] {CompressionCodec.ZSTD, CompressionCodec.SNAPPY, CompressionCodec.LZ4}) {
            assertThat(getCompressor(codec, c)).as("%s", codec).isInstanceOf(ParquetCompressor.AirLiftCompressor.class);
        }
        assertThat(getCompressor(CompressionCodec.GZIP, c)).isInstanceOf(ParquetCompressor.GzipCompressor.class);
        for (CompressionCodec codec : new CompressionCodec[] {CompressionCodec.LZO, CompressionCodec.LZ4_RAW, CompressionCodec.BROTLI}) {
            assertThatThrownBy(() -> getCompressor(codec, c)).hasMessage("Unsupported codec: " + codec);
        }
    }
}
