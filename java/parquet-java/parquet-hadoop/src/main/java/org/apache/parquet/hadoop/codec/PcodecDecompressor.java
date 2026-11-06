/*
 * tparquet fork (java/parquet-java/FORK.md): new file.
 */
package org.apache.parquet.hadoop.codec;

import io.aperiodic.tparquet.Pcodec;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.CodecFactory;

/**
 * Decompresses PCODEC pages (spec/FORMAT.md section 4) to exactly the size Parquet expects; a
 * malformed payload is a {@code PcodecException} (an {@code IOException}). As the reference reader
 * does, an empty values section (expected size 0) is not decoded.
 */
public final class PcodecDecompressor extends CodecFactory.BytesDecompressor {
  @Override
  public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
    if (decompressedSize == 0) {
      return BytesInput.empty();
    }
    return BytesInput.from(Pcodec.decompress(bytes.toByteArray(), decompressedSize));
  }

  /** Decodes {@code compressedSize} bytes at {@code input}'s position into {@code output}; both positions advance. */
  @Override
  public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
      throws IOException {
    if (input.remaining() < compressedSize) {
      throw new IllegalArgumentException("input has " + input.remaining() + " bytes, the page " + compressedSize);
    }
    if (decompressedSize == 0) {
      input.position(input.position() + compressedSize);
      return;
    }
    ByteBuffer page = input.duplicate();
    page.limit(page.position() + compressedSize);
    Pcodec.decompress(page, output, decompressedSize);
    input.position(input.position() + compressedSize);
  }

  @Override
  public void release() {}
}
