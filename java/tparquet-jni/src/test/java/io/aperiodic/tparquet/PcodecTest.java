package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class PcodecTest {
  /** {@code n} PLAIN values of {@code t}: prices with two decimals, or counters. */
  static byte[] plain(PhysicalType t, int n) {
    ByteBuffer b = ByteBuffer.allocate(n * t.width()).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < n; i++) {
      switch (t) {
        case INT32 -> b.putInt(1_000 + 3 * i);
        case INT64 -> b.putLong(1_735_689_600_000L + 37L * i);
        case FLOAT -> b.putFloat((10_000 + i % 500) / 100f);
        case DOUBLE -> b.putDouble((100_000 + i % 977) / 100.0);
      }
    }
    return b.array();
  }

  static byte[] random(int n, long seed) {
    byte[] b = new byte[n];
    new Random(seed).nextBytes(b);
    return b;
  }

  @Test
  void constants() {
    assertThat(Pcodec.CODEC_ID).isEqualTo(20547);
    assertThat(Pcodec.KV_KEY).isEqualTo("parquet.pcodec.version");
    assertThat(Pcodec.KV_VALUE).isEqualTo("1");
    assertThat(Pcodec.DEFAULT_LEVEL).isEqualTo(8);
    assertThat(Pcodec.MAX_LEVEL).isEqualTo(12);
    assertThat(Pcodec.nativeAbiVersion()).isEqualTo(NativeLibrary.ABI_VERSION);
  }

  @Test
  void compressBound() {
    assertThat(Pcodec.compressBound(0)).isEqualTo(2);
    assertThat(Pcodec.compressBound(1000)).isEqualTo(1002);
    assertThat(Pcodec.compressBound(Integer.MAX_VALUE - 2)).isEqualTo(Integer.MAX_VALUE);
    assertThatThrownBy(() -> Pcodec.compressBound(Integer.MAX_VALUE - 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Pcodec.compressBound(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(PhysicalType.class)
  void roundTripsEveryHintLevelAndDecimalFlag(PhysicalType t) throws Exception {
    byte[] src = plain(t, 10_000);
    for (int level : new int[] {0, 1, 8, 12}) {
      for (boolean decimal : new boolean[] {false, true}) {
        byte[] p = Pcodec.compress(src, 0, src.length, t, level, decimal);
        assertThat(p.length).isLessThanOrEqualTo(Pcodec.compressBound(src.length));
        // level 0 is pcodec's fastest setting (0.63 of the input for the floats without DECIMAL)
        assertThat(p.length).as("%s level %d", t, level).isLessThan(level == 0 ? src.length : src.length / 4);
        assertThat(Pcodec.decompress(p, src.length)).isEqualTo(src);
      }
    }
  }

  /** Payload bytes 0, 1 and 2 are the version, the mode and the dtype (spec section 4). */
  @ParameterizedTest
  @EnumSource(PhysicalType.class)
  void theHintReachesTheEncoder(PhysicalType t) throws Exception {
    byte[] p = Pcodec.compress(plain(t, 1000), 0, t.width() * 1000, t, 8, false);
    assertThat(p[0]).isEqualTo((byte) 1);
    assertThat(Pcodec.mode(p)).isEqualTo(Mode.PCO);
    assertThat(p[1]).isEqualTo((byte) Mode.PCO.code());
    assertThat(p[2]).isEqualTo((byte) t.parquetType());
  }

  @ParameterizedTest
  @EnumSource(value = PhysicalType.class, names = {"FLOAT", "DOUBLE"})
  void theDecimalFlagReachesTheEncoder(PhysicalType t) throws Exception {
    byte[] src = plain(t, 5000);
    assertThat(Pcodec.mode(Pcodec.compress(src, 0, src.length, t, 8, true))).isEqualTo(Mode.DECIMAL);
    assertThat(Pcodec.mode(Pcodec.compress(src, 0, src.length, t, 8, false))).isEqualTo(Mode.PCO);
    assertThat(Pcodec.mode(Pcodec.compress(src, t))).isEqualTo(Mode.DECIMAL);
  }

  @Test
  void theLevelReachesTheEncoder() {
    // measured: 8238 bytes at level 1, 1145 at level 8 (without DECIMAL)
    byte[] src = plain(PhysicalType.FLOAT, 10_000);
    byte[] l1 = Pcodec.compress(src, 0, src.length, PhysicalType.FLOAT, 1, false);
    byte[] l8 = Pcodec.compress(src, 0, src.length, PhysicalType.FLOAT, 8, false);
    assertThat(l1.length).isGreaterThan(4 * l8.length);
    assertThat(Pcodec.compress(src, PhysicalType.FLOAT))
        .isEqualTo(Pcodec.compress(src, 0, src.length, PhysicalType.FLOAT, Pcodec.DEFAULT_LEVEL, true));
  }

  @Test
  void withoutAHintCompressibleBytesAreZstd() throws Exception {
    byte[] src = new byte[10_000];
    Arrays.fill(src, (byte) 'a');
    byte[] p = Pcodec.compress(src, null);
    assertThat(Pcodec.mode(p)).isEqualTo(Mode.ZSTD);
    assertThat(Pcodec.decompress(p, src.length)).isEqualTo(src);
  }

  @Test
  void aWidthThatDoesNotDivideTheLengthIsZstd() throws Exception {
    byte[] src = new byte[1001];
    byte[] p = Pcodec.compress(src, PhysicalType.INT64);
    assertThat(Pcodec.mode(p)).isEqualTo(Mode.ZSTD);
    assertThat(Pcodec.decompress(p, 1001)).isEqualTo(src);
  }

  @Test
  void emptyAndIncompressibleInputIsRaw() throws Exception {
    byte[] empty = Pcodec.compress(new byte[0], PhysicalType.DOUBLE);
    assertThat(empty).containsExactly(1, 0);
    assertThat(Pcodec.decompress(empty, 0)).isEmpty();
    byte[] noise = random(4096, 7);
    byte[] p = Pcodec.compress(noise, null);
    assertThat(Pcodec.mode(p)).isEqualTo(Mode.RAW);
    assertThat(p).hasSize(noise.length + 2);
    assertThat(Arrays.copyOfRange(p, 2, p.length)).isEqualTo(noise);
  }

  @Test
  void anOffsetRangeIsCompressedAlone() throws Exception {
    byte[] values = plain(PhysicalType.INT32, 1000);
    byte[] src = new byte[values.length + 10];
    Arrays.fill(src, (byte) 0x55);
    System.arraycopy(values, 0, src, 3, values.length);
    byte[] p = Pcodec.compress(src, 3, values.length, PhysicalType.INT32, 8, true);
    assertThat(p).isEqualTo(Pcodec.compress(values, PhysicalType.INT32));
  }

  @Test
  void decodingIntoARangeLeavesTheRestAlone() throws Exception {
    byte[] values = plain(PhysicalType.DOUBLE, 500);
    byte[] p = Pcodec.compress(values, PhysicalType.DOUBLE);
    byte[] framed = new byte[p.length + 7];
    System.arraycopy(p, 0, framed, 5, p.length);
    byte[] dst = new byte[values.length + 9];
    Arrays.fill(dst, (byte) 0x77);
    Pcodec.decompress(framed, 5, p.length, dst, 4, values.length);
    assertThat(Arrays.copyOfRange(dst, 4, 4 + values.length)).isEqualTo(values);
    for (int i : new int[] {0, 1, 2, 3, 4 + values.length, dst.length - 1}) {
      assertThat(dst[i]).isEqualTo((byte) 0x77);
    }
  }

  @Test
  void theWrongExpectedSizeIsASizeError() {
    byte[] values = plain(PhysicalType.INT64, 1000);
    byte[] p = Pcodec.compress(values, PhysicalType.INT64);
    for (int n : new int[] {values.length - 8, values.length + 8, 0}) {
      assertThatThrownBy(() -> Pcodec.decompress(p, n))
          .isInstanceOfSatisfying(PcodecException.class, e -> assertThat(e.code()).isEqualTo(PcodecException.Code.SIZE));
    }
    // not a multiple of the width: refused before decoding
    assertThatThrownBy(() -> Pcodec.decompress(p, values.length + 1))
        .isInstanceOfSatisfying(PcodecException.class, e -> assertThat(e.code()).isEqualTo(PcodecException.Code.SIZE));
  }

  @Test
  void aCorruptPayloadIsACorruptError() {
    assertThatThrownBy(() -> Pcodec.decompress(new byte[] {2, 0}, 0))
        .isInstanceOfSatisfying(PcodecException.class, e -> {
          assertThat(e.code()).isEqualTo(PcodecException.Code.CORRUPT);
          assertThat(e).hasMessage("pcodec payload: unsupported version 2");
        });
    assertThatThrownBy(() -> Pcodec.decompress(new byte[0], 0))
        .isInstanceOfSatisfying(PcodecException.class, e -> assertThat(e.code()).isEqualTo(PcodecException.Code.CORRUPT));
  }

  @Test
  void modeChecksTheHeader() throws Exception {
    assertThat(Pcodec.mode(new byte[] {1, 3})).isEqualTo(Mode.ZSTD);
    assertThat(Pcodec.mode(new byte[] {1, 2})).isEqualTo(Mode.DECIMAL);
    assertThatThrownBy(() -> Pcodec.mode(new byte[] {1})).isInstanceOf(PcodecException.class).hasMessage("pcodec payload: truncated");
    assertThatThrownBy(() -> Pcodec.mode(new byte[] {1, 9})).hasMessage("pcodec payload: unknown mode 9");
    assertThatThrownBy(() -> Pcodec.mode(new byte[] {0, 0})).hasMessage("pcodec payload: unsupported version 0");
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 13, 255, Integer.MIN_VALUE})
  void aLevelOutOfRangeIsRefused(int level) {
    byte[] src = new byte[8];
    assertThatThrownBy(() -> Pcodec.compress(src, 0, 8, PhysicalType.INT64, level, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("compression level " + level + " outside 0..12");
    assertThatThrownBy(() -> Pcodec.compress(ByteBuffer.wrap(src), ByteBuffer.allocate(10), PhysicalType.INT64, level, true))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /** A bad level is reported first, and leaves the source buffer alone. */
  @Test
  void theLevelIsCheckedBeforeTheBuffers() {
    for (boolean direct : new boolean[] {false, true}) {
      ByteBuffer src = buffer(direct, 80);
      ByteBuffer tooSmall = buffer(direct, 10);
      assertThatThrownBy(() -> Pcodec.compress(src, tooSmall, PhysicalType.INT64, 13, true))
          .isInstanceOf(IllegalArgumentException.class).hasMessage("compression level 13 outside 0..12");
      assertThat(src.position()).isZero();
    }
  }

  @Test
  void rangesOutsideTheArraysAreRefused() {
    byte[] a = new byte[16];
    assertThatThrownBy(() -> Pcodec.compress(a, -1, 4, null, 8, true)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> Pcodec.compress(a, 10, 7, null, 8, true)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> Pcodec.compress(a, 0, -1, null, 8, true)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> Pcodec.decompress(a, 10, 7, a, 0, 1)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> Pcodec.decompress(a, 0, 2, a, 15, 2)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> Pcodec.decompress(a, 0, 2, a, -1, 2)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> Pcodec.decompress(a, -5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Pcodec.compress(null, null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Pcodec.decompress(null, 0)).isInstanceOf(NullPointerException.class);
  }

  // ------------------------------------------------------------------ ByteBuffers

  static ByteBuffer buffer(boolean direct, int capacity) {
    return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
  }

  @Test
  void byteBuffersRoundTripDirectAndHeapInEveryCombination() throws Exception {
    byte[] values = plain(PhysicalType.DOUBLE, 2000);
    byte[] reference = Pcodec.compress(values, 0, values.length, PhysicalType.DOUBLE, 8, true);
    for (boolean srcDirect : new boolean[] {false, true}) {
      for (boolean dstDirect : new boolean[] {false, true}) {
        // non-zero positions and limits short of the capacity on both sides
        ByteBuffer src = buffer(srcDirect, values.length + 20).position(7);
        src.put(values).flip().position(7);
        ByteBuffer dst = buffer(dstDirect, Pcodec.compressBound(values.length) + 30).position(11);
        int n = Pcodec.compress(src, dst, PhysicalType.DOUBLE, 8, true);
        assertThat(n).isEqualTo(reference.length);
        assertThat(src.position()).isEqualTo(src.limit());
        assertThat(dst.position()).isEqualTo(11 + n);
        byte[] payload = new byte[n];
        dst.flip().position(11);
        dst.get(payload);
        assertThat(payload).as("src direct %s, dst direct %s", srcDirect, dstDirect).isEqualTo(reference);

        ByteBuffer in = buffer(srcDirect, n + 3).position(3);
        in.put(payload).flip().position(3);
        ByteBuffer out = buffer(dstDirect, values.length + 5).position(5);
        Pcodec.decompress(in, out, values.length);
        assertThat(in.position()).isEqualTo(in.limit());
        assertThat(out.position()).isEqualTo(5 + values.length);
        byte[] decoded = new byte[values.length];
        out.flip().position(5);
        out.get(decoded);
        assertThat(decoded).isEqualTo(values);
      }
    }
  }

  @Test
  void aReadOnlySourceWorksAndAReadOnlyDestinationIsRefused() throws Exception {
    byte[] values = plain(PhysicalType.INT64, 100);
    ByteBuffer ro = ByteBuffer.wrap(values).asReadOnlyBuffer();
    ByteBuffer dst = ByteBuffer.allocate(values.length + 2);
    int n = Pcodec.compress(ro, dst, PhysicalType.INT64, 8, true);
    ByteBuffer payload = dst.flip().asReadOnlyBuffer();
    ByteBuffer out = ByteBuffer.allocate(values.length);
    Pcodec.decompress(payload, out, values.length);
    assertThat(n).isPositive();
    assertThat(out.array()).isEqualTo(values);

    ByteBuffer roDst = ByteBuffer.allocate(values.length + 2).asReadOnlyBuffer();
    assertThatThrownBy(() -> Pcodec.compress(ByteBuffer.wrap(values), roDst, PhysicalType.INT64, 8, true))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("read-only destination buffer");
    assertThatThrownBy(() -> Pcodec.decompress(ByteBuffer.wrap(new byte[] {1, 0}), ByteBuffer.allocate(1).asReadOnlyBuffer(), 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("read-only destination buffer");
  }

  @Test
  void aDestinationTooSmallIsRefusedBeforeEncoding() {
    ByteBuffer src = ByteBuffer.allocateDirect(80);
    ByteBuffer dst = ByteBuffer.allocateDirect(81);
    assertThatThrownBy(() -> Pcodec.compress(src, dst, PhysicalType.INT64, 8, true))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("destination has 81 bytes, needs 82");
    assertThat(src.position()).isZero();
    assertThatThrownBy(() -> Pcodec.decompress(ByteBuffer.allocate(2), ByteBuffer.allocate(3), 4))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("destination has 3 bytes, needs 4");
    assertThatThrownBy(() -> Pcodec.decompress(ByteBuffer.allocate(2), ByteBuffer.allocate(3), -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aFailedHeapDecodeLeavesBothPositions() {
    ByteBuffer src = ByteBuffer.wrap(new byte[] {1, 7, 0});
    ByteBuffer dst = ByteBuffer.allocate(8);
    assertThatThrownBy(() -> Pcodec.decompress(src, dst, 8)).isInstanceOf(PcodecException.class);
    assertThat(src.position()).isZero();
    assertThat(dst.position()).isZero();
  }

  @Test
  void directDecodingWritesOnlyItsRange() throws Exception {
    byte[] values = plain(PhysicalType.INT32, 64);
    byte[] p = Pcodec.compress(values, PhysicalType.INT32);
    ByteBuffer dst = ByteBuffer.allocateDirect(values.length + 8);
    for (int i = 0; i < dst.capacity(); i++) {
      dst.put(i, (byte) 0x33);
    }
    dst.position(4);
    Pcodec.decompress(ByteBuffer.allocateDirect(p.length).put(p).flip(), dst, values.length);
    assertThat(dst.get(3)).isEqualTo((byte) 0x33);
    assertThat(dst.get(4 + values.length)).isEqualTo((byte) 0x33);
    assertThat(dst.get(4)).isEqualTo(values[0]);
  }

  // ------------------------------------------------------------------ small types

  @Test
  void physicalTypes() {
    assertThat(PhysicalType.fromParquet(1)).isEqualTo(PhysicalType.INT32);
    assertThat(PhysicalType.fromParquet(2)).isEqualTo(PhysicalType.INT64);
    assertThat(PhysicalType.fromParquet(4)).isEqualTo(PhysicalType.FLOAT);
    assertThat(PhysicalType.fromParquet(5)).isEqualTo(PhysicalType.DOUBLE);
    for (int t : new int[] {-1, 0, 3, 6, 7, 99}) {
      assertThat(PhysicalType.fromParquet(t)).isNull();
    }
    assertThat(PhysicalType.fromName("DOUBLE")).isEqualTo(PhysicalType.DOUBLE);
    assertThat(PhysicalType.fromName("INT96")).isNull();
    assertThat(PhysicalType.fromName("BINARY")).isNull();
    assertThat(PhysicalType.INT32.width()).isEqualTo(4);
    assertThat(PhysicalType.INT64.width()).isEqualTo(8);
    assertThat(PhysicalType.FLOAT.width()).isEqualTo(4);
    assertThat(PhysicalType.DOUBLE.width()).isEqualTo(8);
  }

  @Test
  void modes() {
    for (Mode m : Mode.values()) {
      assertThat(Mode.fromCode(m.code())).isEqualTo(m);
    }
    assertThat(Mode.RAW.code()).isZero();
    assertThat(Mode.ZSTD.code()).isEqualTo(3);
    assertThatThrownBy(() -> Mode.fromCode(4)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void exceptionCodes() {
    assertThat(PcodecException.fromNative(2)).isEqualTo(PcodecException.Code.CORRUPT);
    assertThat(PcodecException.fromNative(3)).isEqualTo(PcodecException.Code.SIZE);

    assertThat(new PcodecException(3, "m")).hasMessage("m").extracting(PcodecException::code).isEqualTo(PcodecException.Code.SIZE);
  }
}
