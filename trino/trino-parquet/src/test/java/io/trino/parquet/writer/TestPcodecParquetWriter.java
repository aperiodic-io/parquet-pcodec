/*
 * tparquet fork (trino/trino-parquet/FORK.md): new file.
 */
package io.trino.parquet.writer;

import io.aperiodic.tparquet.Mode;
import io.aperiodic.tparquet.Pcodec;
import io.airlift.units.DataSize;
import io.trino.parquet.MemoryParquetDataSource;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetWriteValidation.ParquetWriteValidationBuilder;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The tparquet fork: Trino's own writer with PCODEC. The required numeric columns (whose V1 pages
 * are arrays of numbers) get PLAIN values without a dictionary and their physical type as the
 * hint (spec section 3); the other columns keep Trino's encodings and get the ZSTD fallback.
 */
class TestPcodecParquetWriter
{
    private static final MessageType SCHEMA = Types.buildMessage()
            .required(INT64).named("ts")
            .required(INT32).named("qty")
            .required(PrimitiveTypeName.DOUBLE).named("price")
            .required(FLOAT).named("ratio")
            .optional(INT64).named("maybe")
            .optional(PrimitiveTypeName.DOUBLE).named("maybe_price")
            .required(BINARY).as(stringType()).named("symbol")
            .required(PrimitiveTypeName.BOOLEAN).named("buy")
            .named("trades");
    private static final List<String> NAMES = List.of("ts", "qty", "price", "ratio", "maybe", "maybe_price", "symbol", "buy");
    private static final List<Type> TYPES = List.of(BIGINT, INTEGER, DOUBLE, REAL, BIGINT, DOUBLE, VARCHAR, BOOLEAN);
    private static final List<String> MODELED = List.of("ts", "qty", "price", "ratio");
    private static final int ROWS = 25_000;

    private static Page page(int offset, int rows)
    {
        BlockBuilder[] b = new BlockBuilder[TYPES.size()];
        for (int c = 0; c < b.length; c++) {
            b[c] = TYPES.get(c).createBlockBuilder(null, rows);
        }
        String[] symbols = {"AAPL", "MSFT", "NVDA"};
        for (int i = offset; i < offset + rows; i++) {
            BIGINT.writeLong(b[0], 1_735_689_600_000L + i * 250L + (i % 3));
            INTEGER.writeLong(b[1], 100 + (i % 50));
            DOUBLE.writeDouble(b[2], 1000 + (i % 400) / 100.0);
            REAL.writeLong(b[3], Float.floatToRawIntBits((i % 64) / 8.0f));
            if (i % 5 == 0) {
                b[4].appendNull();
                b[5].appendNull();
            }
            else {
                // few distinct values: Trino keeps their dictionary
                BIGINT.writeLong(b[4], i % 10);
                DOUBLE.writeDouble(b[5], (i % 10) / 4.0);
            }
            VARCHAR.writeSlice(b[6], utf8Slice(symbols[i % 3]));
            BOOLEAN.writeBoolean(b[7], i % 2 == 0);
        }
        return new Page(Arrays.stream(b).map(BlockBuilder::build).toArray(Block[]::new));
    }

    private record Written(byte[] bytes, ParquetWriter writer) {}

    private static Written write(CompressionCodec codec)
            throws IOException
    {
        Map<List<String>, Type> primitiveTypes = new HashMap<>();
        for (int i = 0; i < NAMES.size(); i++) {
            primitiveTypes.put(List.of(NAMES.get(i)), TYPES.get(i));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetWriter writer = new ParquetWriter(
                out,
                SCHEMA,
                primitiveTypes,
                ParquetWriterOptions.builder()
                        .setMaxPageValueCount(4000)
                        .setMaxRowGroupRowCount(10_000)
                        .setMaxPageSize(DataSize.ofBytes(1 << 20))
                        .build(),
                codec,
                "484",
                Optional.of(DateTimeZone.UTC),
                Optional.of(new ParquetWriteValidationBuilder(TYPES, NAMES)));
        for (int offset = 0; offset < ROWS; offset += 5000) {
            writer.write(page(offset, 5000));
        }
        writer.close();
        return new Written(out.toByteArray(), writer);
    }

    private static ParquetMetadata footer(byte[] file)
            throws IOException
    {
        return MetadataReader.readFooter(new MemoryParquetDataSource(file), Optional.empty());
    }

    /** The payloads of a column chunk's pages, with their page types and where they are in the file. */
    private record PagePayload(PageType type, byte[] payload, int uncompressedSize, int offset) {}

    private static List<PagePayload> pages(byte[] file, ColumnChunkMetadata chunk)
            throws IOException
    {
        long start = chunk.getDictionaryPageOffset() > 0 ? Math.min(chunk.getDictionaryPageOffset(), chunk.getFirstDataPageOffset()) : chunk.getFirstDataPageOffset();
        ByteArrayInputStream in = new ByteArrayInputStream(file, (int) start, (int) chunk.getTotalSize());
        List<PagePayload> out = new ArrayList<>();
        while (in.available() > 0) {
            PageHeader h = Util.readPageHeader(in);
            int offset = (int) (start + chunk.getTotalSize()) - in.available();
            byte[] payload = in.readNBytes(h.getCompressed_page_size());
            out.add(new PagePayload(h.getType(), payload, h.getUncompressed_page_size(), offset));
        }
        return out;
    }

    @Test
    void everyColumnChunkIsPcodecAndTheFooterSaysSo()
            throws IOException
    {
        Written w = write(CompressionCodec.PCODEC);
        ParquetMetadata footer = footer(w.bytes());
        assertThat(footer.getBlocks()).hasSize(3);
        for (BlockMetadata block : footer.getBlocks()) {
            assertThat(block.columns()).hasSize(NAMES.size());
            for (ColumnChunkMetadata c : block.columns()) {
                assertThat(c.getCodec()).isSameAs(CompressionCodecName.PCODEC);
            }
        }
        assertThat(footer.getFileMetaData().getKeyValueMetaData())
                .containsEntry(Pcodec.KV_KEY, Pcodec.KV_VALUE)
                // the key Trino writes already stays
                .containsEntry("writer.time.zone", "UTC");
        assertThat(w.writer().getFileMetaData().getKey_value_metadata()).hasSize(2);
    }

    @Test
    void theModeledColumnsArePlainNumbersCompressedWithTheirType()
            throws IOException
    {
        Written w = write(CompressionCodec.PCODEC);
        for (BlockMetadata block : footer(w.bytes()).getBlocks()) {
            for (ColumnChunkMetadata c : block.columns()) {
                String name = c.getPath().toDotString();
                List<PagePayload> pages = pages(w.bytes(), c);
                if (MODELED.contains(name)) {
                    assertThat(c.getEncodings()).as(name).doesNotContain(Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY).contains(Encoding.PLAIN);
                    assertThat(c.hasDictionaryPage()).as(name).isFalse();
                    for (PagePayload p : pages) {
                        assertThat(p.type()).isSameAs(PageType.DATA_PAGE);
                        Mode mode = Pcodec.mode(p.payload());
                        assertThat(mode).as(name).isIn(Mode.PCO, Mode.DECIMAL);
                        if (name.equals("price")) {
                            assertThat(mode).as(name).isSameAs(Mode.DECIMAL);
                        }
                        assertThat(Pcodec.decompress(p.payload(), p.uncompressedSize())).hasSize(p.uncompressedSize());
                    }
                }
                else {
                    for (PagePayload p : pages) {
                        assertThat(Pcodec.mode(p.payload())).as(name).isIn(Mode.ZSTD, Mode.RAW);
                    }
                    if (name.startsWith("maybe") || name.equals("symbol")) {
                        // Trino's encodings: a dictionary
                        assertThat(c.hasDictionaryPage()).as(name).isTrue();
                    }
                }
            }
        }
    }

    @Test
    void trinoReadsBackWhatItWrote()
            throws IOException
    {
        Written w = write(CompressionCodec.PCODEC);
        // the writer's validation reads the file through ParquetReader and compares every value's checksum
        w.writer().validate(new MemoryParquetDataSource(w.bytes()));
    }

    @Test
    void theOtherCodecsWriteAsUpstream()
            throws IOException
    {
        Written w = write(CompressionCodec.ZSTD);
        ParquetMetadata footer = footer(w.bytes());
        assertThat(footer.getFileMetaData().getKeyValueMetaData()).doesNotContainKey(Pcodec.KV_KEY).containsEntry("writer.time.zone", "UTC");
        ColumnChunkMetadata qty = footer.getBlocks().get(0).columns().get(1);
        assertThat(qty.getCodec()).isSameAs(CompressionCodecName.ZSTD);
        // numbers keep Trino's dictionary encoding
        assertThat(qty.hasDictionaryPage()).isTrue();
        w.writer().validate(new MemoryParquetDataSource(w.bytes()));
    }

    @Test
    void aMalformedPayloadIsACorruptionError()
            throws IOException
    {
        Written w = write(CompressionCodec.PCODEC);
        ColumnChunkMetadata ts = footer(w.bytes()).getBlocks().get(1).columns().get(0);
        PagePayload page = pages(w.bytes(), ts).get(1);
        assertThat(w.bytes()[page.offset()]).isEqualTo((byte) 1);
        for (int version : new int[] {0, 2, 0xff}) {
            byte[] damaged = w.bytes().clone();
            damaged[page.offset()] = (byte) version;
            // PageReader wraps the page's ParquetCorruptionException ("Could not decompress page", upstream)
            assertThatThrownBy(() -> w.writer().validate(new MemoryParquetDataSource(damaged)))
                    .satisfies(e -> assertThat(corruption(e)).hasMessageContaining("Invalid PCODEC page: pcodec payload: unsupported version " + version));
        }
        byte[] damaged = w.bytes().clone();
        damaged[page.offset() + 1] = 9;
        assertThatThrownBy(() -> w.writer().validate(new MemoryParquetDataSource(damaged)))
                .satisfies(e -> assertThat(corruption(e)).hasMessageContaining("Invalid PCODEC page: pcodec payload: unknown mode 9"));
    }

    @Test
    void aDamagedPayloadIsAnErrorEvenWhereItStillDecodes()
            throws IOException
    {
        // a payload has no checksum: damage inside the pcodec stream can decode to other numbers,
        // which Trino's validation catches (as it would for any codec); it is never a crash
        Written w = write(CompressionCodec.PCODEC);
        ColumnChunkMetadata ts = footer(w.bytes()).getBlocks().get(1).columns().get(0);
        for (PagePayload page : pages(w.bytes(), ts)) {
            for (int i = 2; i < page.payload().length; i += Math.max(1, page.payload().length / 16)) {
                byte[] damaged = w.bytes().clone();
                damaged[page.offset() + i] ^= 0x5a;
                assertThatThrownBy(() -> w.writer().validate(new MemoryParquetDataSource(damaged)))
                        .as("byte %s of a page", i)
                        .satisfies(e -> assertThat(corruption(e)).isNotNull());
            }
        }
    }

    /** The ParquetCorruptionException in {@code e}'s cause chain, or null. */
    private static ParquetCorruptionException corruption(Throwable e)
    {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ParquetCorruptionException c) {
                return c;
            }
        }
        return null;
    }

    @Test
    void aFileWithoutColumnChunksHasNoKey()
            throws IOException
    {
        // spec section 2: the key marks files with a PCODEC column chunk (as the Rust writer does)
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Map<List<String>, Type> primitiveTypes = new HashMap<>();
        for (int i = 0; i < NAMES.size(); i++) {
            primitiveTypes.put(List.of(NAMES.get(i)), TYPES.get(i));
        }
        ParquetWriter writer = new ParquetWriter(out, SCHEMA, primitiveTypes, ParquetWriterOptions.builder().build(),
                CompressionCodec.PCODEC, "484", Optional.empty(), Optional.empty());
        writer.close();
        assertThat(footer(out.toByteArray()).getFileMetaData().getKeyValueMetaData()).doesNotContainKey(Pcodec.KV_KEY);
        assertThat(writer.getFileMetaData().getKey_value_metadata()).isNull();
    }
}
