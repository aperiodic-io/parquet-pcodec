package io.aperiodic.tparquet;

import java.io.IOException;

/**
 * A payload that could not be decoded. Thrown by the native library; argument errors are
 * {@link IllegalArgumentException}s and {@link IndexOutOfBoundsException}s, and an internal
 * error of the codec (a caught panic: please report it) is an {@link IllegalStateException}.
 */
public final class PcodecException extends IOException {
  private static final long serialVersionUID = 1L;

  /** What is wrong with the payload, as the C ABI's return codes ({@code rust/capi/include/tparquet.h}). */
  public enum Code {
    /** A malformed payload (C ABI code 2). */
    CORRUPT,
    /** A payload that decodes to a different size than expected (C ABI code 3). */
    SIZE
  }

  private final Code code;

  /** Called by the native library with code 2 (corrupt) or 3 (size). */
  PcodecException(int code, String message) {
    super(message);
    this.code = fromNative(code);
  }

  static Code fromNative(int code) {
    return code == 3 ? Code.SIZE : Code.CORRUPT;
  }

  public Code code() {
    return code;
  }
}
