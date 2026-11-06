package io.aperiodic.tparquet;

/** The payload modes of spec section 4. */
public enum Mode {
  RAW(0),
  PCO(1),
  DECIMAL(2),
  ZSTD(3);

  private final int code;

  Mode(int code) {
    this.code = code;
  }

  /** The mode byte of the payload. */
  public int code() {
    return code;
  }

  static Mode fromCode(int code) {
    for (Mode m : values()) {
      if (m.code == code) {
        return m;
      }
    }
    throw new IllegalStateException("unknown mode code " + code);
  }
}
