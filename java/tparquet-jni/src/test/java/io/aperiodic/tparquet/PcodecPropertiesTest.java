package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

/** Losslessness for any input and hint (spec section 1); no crash on any payload (section 7). */
class PcodecPropertiesTest {
  @Provide
  Arbitrary<PhysicalType> hints() {
    return Arbitraries.of(PhysicalType.class).injectNull(0.2);
  }

  @Property(tries = 300)
  void anyBytesRoundTripWithAnyHint(
      @ForAll @Size(max = 3000) byte[] src,
      @ForAll("hints") PhysicalType hint,
      @ForAll @IntRange(min = 0, max = 12) int level,
      @ForAll boolean decimal)
      throws Exception {
    byte[] p = Pcodec.compress(src, 0, src.length, hint, level, decimal);
    assertThat(p.length).isLessThanOrEqualTo(Pcodec.compressBound(src.length));
    assertThat(Pcodec.decompress(p, src.length)).isEqualTo(src);
  }

  /** Doubles from raw bits: NaN payloads, signed zeros, subnormals and infinities included. */
  @Property(tries = 200)
  void anyDoublesRoundTripBitForBit(@ForAll @Size(max = 2000) long[] bits, @ForAll boolean decimal) throws Exception {
    ByteBuffer b = ByteBuffer.allocate(bits.length * 8).order(ByteOrder.LITTLE_ENDIAN);
    for (long v : bits) {
      b.putLong(v);
    }
    byte[] src = b.array();
    byte[] p = Pcodec.compress(src, 0, src.length, PhysicalType.DOUBLE, 8, decimal);
    assertThat(Pcodec.decompress(p, src.length)).isEqualTo(src);
  }

  /** Short decimals with a few arbitrary values among them (DECIMAL exceptions). */
  @Property(tries = 200)
  void decimalPricesWithExceptionsRoundTrip(
      @ForAll @Size(min = 1, max = 3000) int[] cents, @ForAll @IntRange(min = 0, max = 4) int decimals, @ForAll long noise)
      throws Exception {
    double scale = Math.pow(10, decimals);
    ByteBuffer b = ByteBuffer.allocate(cents.length * 8).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < cents.length; i++) {
      b.putDouble(i % 97 == 13 ? Double.longBitsToDouble(noise + i) : cents[i] / scale);
    }
    byte[] src = b.array();
    assertThat(Pcodec.decompress(Pcodec.compress(src, PhysicalType.DOUBLE), src.length)).isEqualTo(src);
  }

  @Property(tries = 1000)
  void anyPayloadDecodesExactlyOrFails(@ForAll @Size(max = 600) byte[] payload, @ForAll @IntRange(max = 5000) int expected) {
    try {
      assertThat(Pcodec.decompress(payload, expected)).hasSize(expected);
    } catch (PcodecException e) {
      assertThat(e.getMessage()).startsWith("pcodec payload: ");
    }
  }

  /** A valid header and dtype in front of random bytes reaches the pco and zstd decoders. */
  @Property(tries = 1000)
  void aValidHeaderWithGarbageDecodesExactlyOrFails(
      @ForAll @IntRange(min = 0, max = 3) int mode,
      @ForAll("dtypes") int dtype,
      @ForAll @Size(max = 400) byte[] body,
      @ForAll @IntRange(max = 4000) int expected) {
    byte[] payload = new byte[body.length + 3];
    payload[0] = 1;
    payload[1] = (byte) mode;
    payload[2] = (byte) dtype;
    System.arraycopy(body, 0, payload, 3, body.length);
    try {
      assertThat(Pcodec.decompress(payload, expected)).hasSize(expected);
    } catch (PcodecException e) {
      // refused; a panic would be an IllegalStateException and fail the property
    }
  }

  @Provide
  Arbitrary<Integer> dtypes() {
    return Arbitraries.of(1, 2, 4, 5, 0, 3, 200);
  }

  /** Every truncation and every single-byte change of a valid payload decodes exactly or fails. */
  @Property(tries = 100)
  void damagedPayloadsDecodeExactlyOrFail(
      @ForAll("hints") PhysicalType hint, @ForAll @IntRange(min = 1, max = 500) int n, @ForAll int at, @ForAll byte value) {
    byte[] src = PcodecTest.plain(hint == null ? PhysicalType.INT64 : hint, n);
    byte[] p = Pcodec.compress(src, hint);
    int i = Math.floorMod(at, p.length);
    byte[][] damaged = {Arrays.copyOf(p, i), p.clone()};
    damaged[1][i] ^= (byte) (value == 0 ? 1 : value);
    for (byte[] d : damaged) {
      try {
        assertThat(Pcodec.decompress(d, src.length)).hasSize(src.length);
      } catch (PcodecException e) {
        // refused; a panic would be an IllegalStateException and fail the property
      }
    }
  }
}
