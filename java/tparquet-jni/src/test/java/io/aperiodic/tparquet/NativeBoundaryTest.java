package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * The native methods check their arguments again (rust/jni/src/lib.rs): a caller that skips
 * {@link Pcodec}'s checks gets an exception, never a read or write out of bounds or a crash.
 */
class NativeBoundaryTest {
  static Object call(String name, Object... args) throws Throwable {
    for (Method m : Pcodec.class.getDeclaredMethods()) {
      if (m.getName().equals(name)) {
        m.setAccessible(true);
        try {
          return m.invoke(null, args);
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      }
    }
    throw new NoSuchMethodException(name);
  }

  static void refused(Class<? extends Throwable> type, String message, Executable call) {
    assertThatThrownBy(call::execute).isInstanceOf(type).hasMessage(message);
  }

  final byte[] small = new byte[16];
  final byte[] valid = Pcodec.compress(PcodecTest.plain(PhysicalType.INT64, 8), PhysicalType.INT64);

  @Test
  void compressRanges() {
    String oob = "range [-1, -1 + 4) outside 0..16";
    refused(ArrayIndexOutOfBoundsException.class, oob, () -> call("compress0", small, -1, 4, 2, 8, true));
    refused(ArrayIndexOutOfBoundsException.class, "range [2, 2 + 15) outside 0..16", () -> call("compress0", small, 2, 15, 2, 8, true));
    refused(ArrayIndexOutOfBoundsException.class, "range [0, 0 + -1) outside 0..16", () -> call("compress0", small, 0, -1, 2, 8, true));
  }

  @Test
  void compressLevels() throws Throwable {
    refused(IllegalArgumentException.class, "pcodec payload: compression level 13 above 12", () -> call("compress0", small, 0, 16, 2, 13, true));
    refused(IllegalArgumentException.class, "compression level 256 outside 0..12", () -> call("compress0", small, 0, 16, 2, 256, true));
    refused(IllegalArgumentException.class, "compression level -1 outside 0..12", () -> call("compress0", small, 0, 16, 2, -1, true));
    assertThat((byte[]) call("compress0", small, 0, 16, 2, 12, true)).isEqualTo(Pcodec.compress(small, 0, 16, PhysicalType.INT64, 12, true));
    // any type code outside 1, 2, 4, 5 means no hint
    assertThat((byte[]) call("compress0", small, 0, 16, 99, 8, false)).isEqualTo(Pcodec.compress(small, 0, 16, null, 8, false));
  }

  @Test
  void compressDirectRanges() throws Throwable {
    ByteBuffer src = ByteBuffer.allocateDirect(16);
    ByteBuffer dst = ByteBuffer.allocateDirect(32);
    refused(ArrayIndexOutOfBoundsException.class, "range [8, 8 + 9) outside 0..16",
        () -> call("compressDirect0", src, 8, 9, 2, 8, true, dst, 0, 32));
    refused(ArrayIndexOutOfBoundsException.class, "range [1, 1 + 32) outside 0..32",
        () -> call("compressDirect0", src, 0, 16, 2, 8, true, dst, 1, 32));
    refused(IllegalArgumentException.class, "destination has 17 bytes, needs 18",
        () -> call("compressDirect0", src, 0, 16, 2, 8, true, dst, 0, 17));
    assertThat((int) call("compressDirect0", src, 0, 16, 2, 8, true, dst, 0, 18)).isPositive();
    refused(IllegalArgumentException.class, "pcodec payload: compression level 99 above 12",
        () -> call("compressDirect0", src, 0, 16, 2, 99, true, dst, 0, 32));
  }

  @Test
  void compressDirectRefusesOverlap() throws Throwable {
    ByteBuffer buf = ByteBuffer.allocateDirect(64);
    refused(IllegalArgumentException.class, "source and destination overlap",
        () -> call("compressDirect0", buf, 0, 16, 2, 8, true, buf, 10, 20));
    // adjacent ranges of one buffer are fine
    assertThat((int) call("compressDirect0", buf, 0, 16, 2, 8, true, buf, 16, 18)).isPositive();
  }

  @Test
  void heapBuffersAreNotDirect() {
    assertThatThrownBy(() -> call("compressDirect0", ByteBuffer.allocate(16), 0, 16, 2, 8, true, ByteBuffer.allocateDirect(32), 0, 32))
        .isInstanceOf(IllegalStateException.class).hasMessageStartingWith("tparquet JNI: ");
    assertThatThrownBy(() -> call("decompressDirect0", ByteBuffer.allocateDirect(2), 0, 2, ByteBuffer.allocate(4), 0, 0))
        .isInstanceOf(IllegalStateException.class).hasMessageStartingWith("tparquet JNI: ");
  }

  @Test
  void decompressRanges() throws Throwable {
    byte[] dst = new byte[64];
    // the destination range is checked before anything is allocated for it
    refused(ArrayIndexOutOfBoundsException.class, "range [0, 0 + 2147483647) outside 0..64",
        () -> call("decompress0", valid, 0, valid.length, dst, 0, Integer.MAX_VALUE));
    refused(ArrayIndexOutOfBoundsException.class, "range [1, 1 + 64) outside 0..64",
        () -> call("decompress0", valid, 0, valid.length, dst, 1, 64));
    refused(ArrayIndexOutOfBoundsException.class, "range [-3, -3 + 64) outside 0..64",
        () -> call("decompress0", valid, 0, valid.length, dst, -3, 64));
    refused(ArrayIndexOutOfBoundsException.class, "range [1, 1 + " + valid.length + ") outside 0.." + valid.length,
        () -> call("decompress0", valid, 1, valid.length, dst, 0, 64));
    call("decompress0", valid, 0, valid.length, dst, 0, 64);
    assertThat(dst).isEqualTo(PcodecTest.plain(PhysicalType.INT64, 8));
  }

  @Test
  void decompressDirectRangesAndOverlap() throws Throwable {
    ByteBuffer src = ByteBuffer.allocateDirect(valid.length).put(valid).flip();
    ByteBuffer dst = ByteBuffer.allocateDirect(64);
    refused(ArrayIndexOutOfBoundsException.class, "range [1, 1 + " + valid.length + ") outside 0.." + valid.length,
        () -> call("decompressDirect0", src, 1, valid.length, dst, 0, 64));
    refused(ArrayIndexOutOfBoundsException.class, "range [8, 8 + 64) outside 0..64",
        () -> call("decompressDirect0", src, 0, valid.length, dst, 8, 64));
    ByteBuffer one = ByteBuffer.allocateDirect(128).put(valid).flip().limit(128);
    refused(IllegalArgumentException.class, "source and destination overlap",
        () -> call("decompressDirect0", one, 0, valid.length, one, valid.length - 1, 64));
    call("decompressDirect0", one, 0, valid.length, one, valid.length, 64);
    byte[] got = new byte[64];
    one.position(valid.length);
    one.get(got);
    assertThat(got).isEqualTo(PcodecTest.plain(PhysicalType.INT64, 8));
  }

  @Test
  void emptyDirectRanges() throws Throwable {
    ByteBuffer empty = ByteBuffer.allocateDirect(0);
    ByteBuffer dst = ByteBuffer.allocateDirect(2);
    assertThat((int) call("compressDirect0", empty, 0, 0, 2, 8, true, dst, 0, 2)).isEqualTo(2);
    ByteBuffer payload = ByteBuffer.allocateDirect(2).put(new byte[] {1, 0}).flip();
    call("decompressDirect0", payload, 0, 2, empty, 0, 0);
  }

  @Test
  void modeRangesAndValue() throws Throwable {
    refused(ArrayIndexOutOfBoundsException.class, "range [0, 0 + 17) outside 0..16", () -> call("mode0", small, 0, 17));
    assertThat((int) call("mode0", valid, 0, valid.length)).isEqualTo(Mode.PCO.code());
    assertThat((int) call("mode0", new byte[] {9, 1, 3}, 1, 2)).isEqualTo(Mode.ZSTD.code());
  }

  @Test
  void nullArraysAreErrorsNotCrashes() {
    assertThatThrownBy(() -> call("compress0", null, 0, 0, 2, 8, true)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> call("decompress0", valid, 0, valid.length, null, 0, 0)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> call("mode0", null, 0, 0)).isInstanceOf(RuntimeException.class);
  }

  @Test
  void aPanicBecomesAnIllegalStateException() throws Exception {
    for (int i = 0; i < 3; i++) {
      assertThatThrownBy(NativeTestHooks::panic0).isExactlyInstanceOf(IllegalStateException.class).hasMessage("internal panic in tparquet");
    }
    // the JVM and the library carry on
    assertThat(Pcodec.decompress(valid, 64)).isEqualTo(PcodecTest.plain(PhysicalType.INT64, 8));
  }
}
