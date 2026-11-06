package io.aperiodic.tparquet;

/** The Parquet physical types PCODEC models (spec section 4), with their Parquet {@code Type} values. */
public enum PhysicalType {
  INT32(1, 4),
  INT64(2, 8),
  FLOAT(4, 4),
  DOUBLE(5, 8);

  private final int parquetType;
  private final int width;

  PhysicalType(int parquetType, int width) {
    this.parquetType = parquetType;
    this.width = width;
  }

  /** The Parquet {@code Type} value (1, 2, 4 or 5). */
  public int parquetType() {
    return parquetType;
  }

  /** Bytes per value in Parquet {@code PLAIN} encoding. */
  public int width() {
    return width;
  }

  /**
   * The type of a Parquet {@code Type} value, or {@code null} for the types PCODEC does not model
   * ({@code BOOLEAN}, {@code INT96}, {@code BYTE_ARRAY}, {@code FIXED_LEN_BYTE_ARRAY}, others).
   */
  public static PhysicalType fromParquet(int parquetType) {
    for (PhysicalType t : values()) {
      if (t.parquetType == parquetType) {
        return t;
      }
    }
    return null;
  }

  /** The type of a parquet-java {@code PrimitiveTypeName} name, or {@code null} for the others. */
  public static PhysicalType fromName(String primitiveTypeName) {
    for (PhysicalType t : values()) {
      if (t.name().equals(primitiveTypeName)) {
        return t;
      }
    }
    return null;
  }
}
