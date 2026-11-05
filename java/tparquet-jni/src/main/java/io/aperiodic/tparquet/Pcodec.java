package io.aperiodic.tparquet;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The PCODEC Parquet compression codec: the page payload of {@code spec/FORMAT.md}, over the
 * JNI library {@code rust/jni} (the same encoder and decoder as every other tparquet package).
 *
 * <p>{@code compress} turns the bytes Parquet would compress into a payload; {@code decompress}
 * turns a payload back into exactly those bytes, and fails unless it produces exactly the
 * expected size. Decoding never needs the column type; the type hint given to {@code compress}
 * only affects the ratio. Payloads are untrusted input: a malformed one raises a {@link
 * PcodecException}, never a crash. All methods are thread-safe.
 */
public final class Pcodec {
  /** {@code CompressionCodec} value of PCODEC in the Parquet footer (spec section 2). */
  public static final int CODEC_ID = 20547;
  /** Footer key-value pair writers add to files with PCODEC columns (spec section 2). */
  public static final String KV_KEY = "parquet.pcodec.version";
  /** Value of {@link #KV_KEY}. */
  public static final String KV_VALUE = "1";
  /** pcodec compression level used when none is given. */
  public static final int DEFAULT_LEVEL = 8;
  /** Highest pcodec compression level. */
  public static final int MAX_LEVEL = 12;

  static {
    NativeLibrary.load();
  }

  private Pcodec() {}

  /** Largest payload {@code compress} writes for {@code srcLen} input bytes. */
  public static int compressBound(int srcLen) {
    if (srcLen < 0 || srcLen > Integer.MAX_VALUE - 2) {
      throw new IllegalArgumentException("input length " + srcLen + " out of range");
    }
    return srcLen + 2;
  }

  /** Encodes {@code src} with the default level, trying DECIMAL mode for floats. */
  public static byte[] compress(byte[] src, PhysicalType hint) {
    return compress(src, 0, src.length, hint, DEFAULT_LEVEL, true);
  }

  /**
   * Encodes {@code src[off, off + len)}; a range outside the array is an {@link
   * ArrayIndexOutOfBoundsException}.
   *
   * @param hint the column's physical type, or {@code null} for none (the ZSTD fallback)
   * @param level pcodec level 0..{@value #MAX_LEVEL}
   * @param decimal whether to try DECIMAL mode for {@code FLOAT} and {@code DOUBLE}
   */
  public static byte[] compress(byte[] src, int off, int len, PhysicalType hint, int level, boolean decimal) {
    checkLevel(level);
    return compress0(Objects.requireNonNull(src, "src"), off, len, typeCode(hint), level, decimal);
  }

  /**
   * Encodes the remaining bytes of {@code src} into {@code dst} at its position; both positions
   * advance. {@code dst} needs {@link #compressBound} of {@code src.remaining()} bytes remaining.
   *
   * @return the payload length
   */
  public static int compress(ByteBuffer src, ByteBuffer dst, PhysicalType hint, int level, boolean decimal) {
    checkLevel(level);
    int len = src.remaining();
    int bound = compressBound(len);
    if (dst.isReadOnly()) {
      throw new IllegalArgumentException("read-only destination buffer");
    }
    if (dst.remaining() < bound) {
      throw new IllegalArgumentException("destination has " + dst.remaining() + " bytes, needs " + bound);
    }
    int written;
    if (src.isDirect() && dst.isDirect()) {
      written = compressDirect0(src, src.position(), len, typeCode(hint), level, decimal, dst, dst.position(), dst.remaining());
      src.position(src.limit());
      dst.position(dst.position() + written);
    } else {
      byte[] payload = compress(bytes(src), 0, len, hint, level, decimal);
      dst.put(payload);
      written = payload.length;
    }
    return written;
  }

  /** Decodes {@code payload}, which must produce exactly {@code expectedLen} bytes. */
  public static byte[] decompress(byte[] payload, int expectedLen) throws PcodecException {
    if (expectedLen < 0) {
      throw new IllegalArgumentException("negative expected length " + expectedLen);
    }
    byte[] dst = new byte[expectedLen];
    decompress(payload, 0, payload.length, dst, 0, expectedLen);
    return dst;
  }

  /**
   * Decodes {@code payload[off, off + len)} into {@code dst[dstOff, dstOff + dstLen)}, which it
   * fills exactly. A range outside its array is an {@link ArrayIndexOutOfBoundsException}.
   */
  public static void decompress(byte[] payload, int off, int len, byte[] dst, int dstOff, int dstLen)
      throws PcodecException {
    decompress0(Objects.requireNonNull(payload, "payload"), off, len, Objects.requireNonNull(dst, "dst"), dstOff, dstLen);
  }

  /**
   * Decodes the remaining bytes of {@code src} into the next {@code expectedLen} bytes of {@code
   * dst}; both positions advance (only on success).
   */
  public static void decompress(ByteBuffer src, ByteBuffer dst, int expectedLen) throws PcodecException {
    if (expectedLen < 0) {
      throw new IllegalArgumentException("negative expected length " + expectedLen);
    }
    if (dst.isReadOnly()) {
      throw new IllegalArgumentException("read-only destination buffer");
    }
    if (dst.remaining() < expectedLen) {
      throw new IllegalArgumentException("destination has " + dst.remaining() + " bytes, needs " + expectedLen);
    }
    int len = src.remaining();
    if (src.isDirect() && dst.isDirect()) {
      decompressDirect0(src, src.position(), len, dst, dst.position(), expectedLen);
      src.position(src.limit());
      dst.position(dst.position() + expectedLen);
    } else {
      ByteBuffer in = src.duplicate();
      byte[] out = decompress(bytes(in), expectedLen);
      src.position(src.limit());
      dst.put(out);
    }
  }

  /** The mode of a payload (its first two bytes, checked; the body is not decoded). */
  public static Mode mode(byte[] payload) throws PcodecException {
    return Mode.fromCode(mode0(payload, 0, payload.length));
  }

  static int nativeAbiVersion() {
    return abiVersion0();
  }

  private static void checkLevel(int level) {
    if (level < 0 || level > MAX_LEVEL) {
      throw new IllegalArgumentException("compression level " + level + " outside 0.." + MAX_LEVEL);
    }
  }

  private static int typeCode(PhysicalType hint) {
    return hint == null ? -1 : hint.parquetType();
  }

  /** The remaining bytes of {@code b}, consuming them. */
  private static byte[] bytes(ByteBuffer b) {
    byte[] a = new byte[b.remaining()];
    b.get(a);
    return a;
  }

  private static native int abiVersion0();

  private static native byte[] compress0(byte[] src, int off, int len, int physicalType, int level, boolean decimal);

  private static native int compressDirect0(
      ByteBuffer src, int srcPos, int srcLen, int physicalType, int level, boolean decimal, ByteBuffer dst, int dstPos, int dstCap);

  private static native void decompress0(byte[] src, int off, int len, byte[] dst, int dstOff, int dstLen)
      throws PcodecException;

  private static native void decompressDirect0(ByteBuffer src, int srcPos, int srcLen, ByteBuffer dst, int dstPos, int dstLen)
      throws PcodecException;

  private static native int mode0(byte[] src, int off, int len) throws PcodecException;
}
