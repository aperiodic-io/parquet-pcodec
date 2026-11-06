/*
 * tparquet fork (trino/trino-parquet/FORK.md): new file.
 */
package io.trino.parquet;

import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.aperiodic.tparquet.Pcodec;
import io.aperiodic.tparquet.PcodecException;
import io.aperiodic.tparquet.PhysicalType;
import io.trino.parquet.PcodecTestdata.Payload;
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static io.trino.parquet.ParquetCompressionUtils.decompress;
import static org.apache.parquet.format.CompressionCodec.PCODEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The tparquet fork: ParquetCompressionUtils.decompress for PCODEC (spec/FORMAT.md sections 3 and 4). */
class TestPcodecDecompression
{
    private static final ParquetDataSourceId ID = new ParquetDataSourceId("file:///test.parquet");

    static List<Payload> valid()
    {
        return PcodecTestdata.valid();
    }

    static List<Payload> invalid()
    {
        return PcodecTestdata.invalid();
    }

    /** The invalid vectors that expect bytes: a Parquet page with nothing to decode is not decoded (below). */
    static List<Payload> invalidWithExpectedBytes()
    {
        return invalid().stream().filter(p -> p.expectedLen() > 0).toList();
    }

    @Test
    void everyPayloadVectorIsHere()
    {
        assertThat(valid()).hasSizeGreaterThanOrEqualTo(14);
        assertThat(invalidWithExpectedBytes()).hasSizeGreaterThanOrEqualTo(19);
        assertThat(valid().stream().map(Payload::mode).distinct()).containsExactlyInAnyOrder("RAW", "PCO", "DECIMAL", "ZSTD");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("valid")
    void decodesEveryValidPayloadVector(Payload p)
            throws IOException
    {
        Slice out = decompress(ID, PCODEC, Slices.wrappedBuffer(p.payload()), p.expectedLen());
        assertThat(out.length()).isEqualTo(p.expectedLen());
        assertThat(out.getBytes()).isEqualTo(p.decoded());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidWithExpectedBytes")
    void refusesEveryInvalidPayloadVector(Payload p)
    {
        assertThatThrownBy(() -> decompress(ID, PCODEC, Slices.wrappedBuffer(p.payload()), p.expectedLen()))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageStartingWith("Malformed Parquet file. Invalid PCODEC page: ")
                // the reference decoder's message, and the file
                .hasMessageContaining(p.error())
                .hasMessageEndingWith("[" + ID + "]")
                .hasCauseInstanceOf(PcodecException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("valid")
    void refusesAnyOtherExpectedSize(Payload p)
    {
        for (int size : new int[] {p.expectedLen() + 1, p.expectedLen() + 8, p.expectedLen() - 1}) {
            if (size <= 0) {
                continue;
            }
            assertThatThrownBy(() -> decompress(ID, PCODEC, Slices.wrappedBuffer(p.payload()), size))
                    .as("%s with expected size %s", p.name(), size)
                    .isInstanceOf(ParquetCorruptionException.class)
                    .hasMessageContaining("Invalid PCODEC page");
        }
    }

    @Test
    void aNegativeExpectedSizeIsCorruption()
    {
        Payload p = valid().get(1);
        assertThatThrownBy(() -> decompress(ID, PCODEC, Slices.wrappedBuffer(p.payload()), -1))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessage("Malformed Parquet file. Invalid PCODEC page: negative uncompressed size -1 [" + ID + "]");
    }

    @Test
    void anEmptyPageWithBytesExpectedIsCorruption()
    {
        // Trino returns an empty slice for empty input of the other codecs; PCODEC payloads are never empty
        assertThatThrownBy(() -> decompress(ID, PCODEC, Slices.EMPTY_SLICE, 16))
                .isInstanceOf(ParquetCorruptionException.class)
                .hasMessageContaining("pcodec payload: truncated");
    }

    @Test
    void aPageWithNothingExpectedIsNotDecoded()
            throws IOException
    {
        // as the reference reader: an empty values section (a V2 page of nulls) is not decoded,
        // whatever its compressed bytes
        for (Payload p : invalid()) {
            if (p.expectedLen() == 0) {
                assertThat(decompress(ID, PCODEC, Slices.wrappedBuffer(p.payload()), 0).length()).as(p.name()).isZero();
            }
        }
        assertThat(decompress(ID, PCODEC, Slices.EMPTY_SLICE, 0).length()).isZero();
    }

    @Test
    void decodesASliceOfALargerArray()
            throws IOException
    {
        Payload p = valid().stream().filter(v -> v.mode().equals("PCO")).findFirst().orElseThrow();
        byte[] padded = new byte[p.payload().length + 37];
        Arrays.fill(padded, (byte) 0x5a);
        System.arraycopy(p.payload(), 0, padded, 13, p.payload().length);
        Slice slice = Slices.wrappedBuffer(padded, 13, p.payload().length);
        assertThat(decompress(ID, PCODEC, slice, p.expectedLen()).getBytes()).isEqualTo(p.decoded());
    }

    @Test
    void roundTripsWhatTheCodecWrites()
            throws IOException
    {
        Random random = new Random(20547);
        for (PhysicalType hint : new PhysicalType[] {null, PhysicalType.INT32, PhysicalType.INT64, PhysicalType.FLOAT, PhysicalType.DOUBLE}) {
            for (int n : new int[] {1, 7, 8, 1000, 65536}) {
                byte[] data = new byte[n];
                random.nextBytes(data);
                Slice out = decompress(ID, PCODEC, Slices.wrappedBuffer(Pcodec.compress(data, hint)), n);
                assertThat(out.getBytes()).as("%s %s", hint, n).isEqualTo(data);
            }
        }
    }

    /** Hadoop's LZ4 framing: per block of {@code block} bytes, its size and one compressed chunk, both lengths big-endian. */
    private static byte[] lz4Framed(byte[] data, int block)
    {
        io.airlift.compress.v3.lz4.Lz4Compressor lz4 = io.airlift.compress.v3.lz4.Lz4Compressor.create();
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (int off = 0; off < data.length; off += block) {
            int n = Math.min(block, data.length - off);
            byte[] chunk = new byte[lz4.maxCompressedLength(n)];
            int len = lz4.compress(data, off, n, chunk, 0, chunk.length);
            out.writeBytes(java.nio.ByteBuffer.allocate(8).putInt(n).putInt(len).array());
            out.write(chunk, 0, len);
        }
        return out.toByteArray();
    }

    @Test
    void theOtherCodecsAreUnchanged()
            throws IOException
    {
        byte[] data = new byte[4096];
        new Random(1).nextBytes(data);
        ZstdCompressor zstd = ZstdCompressor.create();
        byte[] compressed = new byte[zstd.maxCompressedLength(data.length)];
        int len = zstd.compress(data, 0, data.length, compressed, 0, compressed.length);
        assertThat(decompress(ID, CompressionCodec.ZSTD, Slices.wrappedBuffer(compressed, 0, len), data.length).getBytes()).isEqualTo(data);
        // at an offset of a larger array
        byte[] padded = new byte[len + 11];
        System.arraycopy(compressed, 0, padded, 5, len);
        assertThat(decompress(ID, CompressionCodec.ZSTD, Slices.wrappedBuffer(padded, 5, len), data.length).getBytes()).isEqualTo(data);
        SnappyCompressor snappy = SnappyCompressor.create();
        byte[] snappyCompressed = new byte[snappy.maxCompressedLength(data.length)];
        int snappyLen = snappy.compress(data, 0, data.length, snappyCompressed, 0, snappyCompressed.length);
        assertThat(decompress(ID, CompressionCodec.SNAPPY, Slices.wrappedBuffer(snappyCompressed, 0, snappyLen), data.length).getBytes()).isEqualTo(data);
        assertThatThrownBy(() -> decompress(ID, CompressionCodec.SNAPPY, Slices.wrappedBuffer(snappyCompressed, 0, snappyLen), data.length + 1))
                .hasMessageContaining("Invalid uncompressedSize for SNAPPY input");
        assertThat(decompress(ID, CompressionCodec.UNCOMPRESSED, Slices.wrappedBuffer(data), data.length).getBytes()).isEqualTo(data);
        // empty input of a standard codec is still an empty slice, as upstream
        assertThat(decompress(ID, CompressionCodec.ZSTD, Slices.EMPTY_SLICE, 16).length()).isZero();
        // Hadoop-framed LZ4 with two blocks: the second decodes at an offset of the output
        assertThat(decompress(ID, CompressionCodec.LZ4, Slices.wrappedBuffer(lz4Framed(data, 1000)), data.length).getBytes()).isEqualTo(data);
        for (CompressionCodec codec : new CompressionCodec[] {CompressionCodec.BROTLI, CompressionCodec.LZ4_RAW}) {
            assertThatThrownBy(() -> decompress(ID, codec, Slices.wrappedBuffer(data), data.length))
                    .isInstanceOf(ParquetCorruptionException.class)
                    .hasMessageContaining("Codec not supported in Parquet: " + codec);
        }
    }
}
