package io.aperiodic.tparquet;

/** Native test hooks of {@code rust/jni} (not part of the API). */
final class NativeTestHooks {
  static {
    NativeLibrary.load();
  }

  private NativeTestHooks() {}

  /** Panics inside the library's guard; returns -1 with a pending {@link IllegalStateException}. */
  static native int panic0();
}
