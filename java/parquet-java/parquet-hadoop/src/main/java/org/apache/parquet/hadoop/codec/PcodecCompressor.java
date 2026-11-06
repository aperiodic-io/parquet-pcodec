/*
 * tparquet fork (java/parquet-java/FORK.md): new file.
 */
package org.apache.parquet.hadoop.codec;

import io.aperiodic.tparquet.Pcodec;
import io.aperiodic.tparquet.PhysicalType;
import java.io.IOException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Compresses pages with PCODEC (spec/FORMAT.md section 4). The factory's instance has no type
 * hint; {@link #forColumn} gives each column chunk's page writer one with the column's physical
 * type, which only changes the compression ratio (section 1).
 */
public final class PcodecCompressor extends CodecFactory.BytesCompressor {
  private final int level;
  private final boolean decimal;
  private final PhysicalType hint;

  PcodecCompressor(int level, boolean decimal, PhysicalType hint) {
    this.level = level;
    this.decimal = decimal;
    this.hint = hint;
  }

  /** {@code compressor} for the pages of {@code column}: typed if it is a PCODEC compressor. */
  public static BytesInputCompressor forColumn(BytesInputCompressor compressor, ColumnDescriptor column) {
    if (!(compressor instanceof PcodecCompressor)) {
      return compressor;
    }
    PcodecCompressor c = (PcodecCompressor) compressor;
    String type = column.getPrimitiveType().getPrimitiveTypeName().name();
    return new PcodecCompressor(c.level, c.decimal, PhysicalType.fromName(type));
  }

  @Override
  public BytesInput compress(BytesInput bytes) throws IOException {
    byte[] src = bytes.toByteArray();
    return BytesInput.from(Pcodec.compress(src, 0, src.length, hint, level, decimal));
  }

  @Override
  public CompressionCodecName getCodecName() {
    return CompressionCodecName.PCODEC;
  }

  @Override
  public void release() {}

  public int level() {
    return level;
  }

  public boolean decimal() {
    return decimal;
  }

  /** The type hint, or {@code null} for none. */
  public PhysicalType hint() {
    return hint;
  }
}
