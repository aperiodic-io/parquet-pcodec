/*
 * tparquet fork (java/parquet-java/FORK.md): new file.
 */
package org.apache.parquet.hadoop.codec;

import io.aperiodic.tparquet.Pcodec;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * The Hadoop codec class of {@link CompressionCodecName#PCODEC}, the codec of spec/FORMAT.md.
 *
 * <p>PCODEC needs each page's physical type (to compress) and expected size (to decompress), which
 * the Hadoop stream API does not carry, so this class refuses streams: {@code CodecFactory} creates
 * a {@link PcodecCompressor} and a {@link PcodecDecompressor} for PCODEC instead. A code path that
 * reached the stream API would fail here rather than write or read wrong bytes.
 */
public class PcodecCodec implements Configurable, CompressionCodec {
  /** pcodec compression level, 0..12. */
  public static final String LEVEL = "parquet.compression.codec.pcodec.level";
  /** Whether to try DECIMAL mode for FLOAT and DOUBLE columns. */
  public static final String DECIMAL = "parquet.compression.codec.pcodec.decimal";

  public static final int DEFAULT_LEVEL = Pcodec.DEFAULT_LEVEL;
  public static final boolean DEFAULT_DECIMAL = true;

  private static final String STREAMS =
      "PCODEC needs each page's type and expected size: use CodecFactory, not the Hadoop stream API";

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public Compressor createCompressor() {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public Decompressor createDecompressor() {
    throw new UnsupportedOperationException(STREAMS);
  }

  @Override
  public String getDefaultExtension() {
    return CompressionCodecName.PCODEC.getExtension();
  }

  /** The compressor of a codec factory: level and decimal flag from {@link #LEVEL} and {@link #DECIMAL}. */
  public static PcodecCompressor compressor(ParquetConfiguration conf) {
    int level = conf.getInt(LEVEL, DEFAULT_LEVEL);
    if (level < 0 || level > Pcodec.MAX_LEVEL) {
      throw new IllegalArgumentException(LEVEL + "=" + level + ": the level is 0.." + Pcodec.MAX_LEVEL);
    }
    return new PcodecCompressor(level, conf.getBoolean(DECIMAL, DEFAULT_DECIMAL), null);
  }

  /**
   * The footer's key-value metadata with {@code parquet.pcodec.version = "1"} added when a column
   * chunk uses PCODEC and the key is not there (spec section 2); otherwise {@code kv} itself.
   */
  public static Map<String, String> withVersion(Map<String, String> kv, List<BlockMetaData> blocks) {
    if (kv.containsKey(Pcodec.KV_KEY) || !usesPcodec(blocks)) {
      return kv;
    }
    Map<String, String> out = new HashMap<>(kv);
    out.put(Pcodec.KV_KEY, Pcodec.KV_VALUE);
    return out;
  }

  private static boolean usesPcodec(List<BlockMetaData> blocks) {
    for (BlockMetaData b : blocks) {
      for (ColumnChunkMetaData c : b.getColumns()) {
        if (c.getCodec() == CompressionCodecName.PCODEC) {
          return true;
        }
      }
    }
    return false;
  }
}
