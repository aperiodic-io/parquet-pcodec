/*
 * tparquet fork (trino/trino-parquet/FORK.md): new file.
 */
package io.trino.parquet;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.PcodecTestdata.Payload;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import java.util.List;

import static io.trino.parquet.ParquetCompressionUtils.decompress;
import static org.apache.parquet.format.CompressionCodec.PCODEC;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Payloads are untrusted (spec section 7): whatever the bytes, decompress returns exactly the
 * expected size or throws a ParquetCorruptionException, never anything else.
 */
class TestPcodecDecompressionProperties
{
    private static final ParquetDataSourceId ID = new ParquetDataSourceId("property");
    private static final List<Payload> VALID = PcodecTestdata.valid();

    /** The result of a decompress call: the output's length, or -1 for a ParquetCorruptionException. */
    private static int outcome(byte[] payload, int expected)
    {
        try {
            Slice out = decompress(ID, PCODEC, Slices.wrappedBuffer(payload), expected);
            return out.length();
        }
        catch (ParquetCorruptionException e) {
            assertThat(e).hasMessageContaining("Invalid PCODEC page");
            return -1;
        }
        catch (Exception e) {
            throw new AssertionError("not a ParquetCorruptionException: " + e, e);
        }
    }

    @Property(tries = 2000)
    void randomBytes(@ForAll @Size(max = 300) byte[] payload, @ForAll @IntRange(min = 1, max = 1 << 16) int expected)
    {
        assertThat(outcome(payload, expected)).isIn(-1, expected);
    }

    @Property(tries = 2000)
    void randomBodiesBehindAValidHeader(@ForAll @IntRange(min = 0, max = 3) int mode, @ForAll @IntRange(min = 1, max = 5) int dtype,
            @ForAll @Size(max = 200) byte[] body, @ForAll @IntRange(min = 1, max = 4096) int expected)
    {
        byte[] payload = new byte[body.length + 3];
        payload[0] = 1;
        payload[1] = (byte) mode;
        payload[2] = (byte) dtype;
        System.arraycopy(body, 0, payload, 3, body.length);
        assertThat(outcome(payload, expected)).isIn(-1, expected);
    }

    @Provide
    Arbitrary<Payload> validPayload()
    {
        return Arbitraries.of(VALID).filter(p -> p.expectedLen() > 0);
    }

    @Property(tries = 3000)
    void damagedValidPayloads(@ForAll("validPayload") Payload p, @ForAll @IntRange(min = 0, max = 1 << 20) int at, @ForAll byte xor,
            @ForAll @IntRange(min = 0, max = 3) int cut)
    {
        byte[] damaged = p.payload().clone();
        int i = at % damaged.length;
        damaged[i] ^= (byte) (xor == 0 ? 1 : xor);
        if (cut > 0) {
            damaged = java.util.Arrays.copyOf(damaged, Math.max(0, damaged.length - cut * (1 + at % 7)));
        }
        int got = outcome(damaged, p.expectedLen());
        assertThat(got).isIn(-1, p.expectedLen());
    }
}
