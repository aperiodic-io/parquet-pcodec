package org.apache.parquet.hadoop;

import java.io.IOException;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

/**
 * Part of tparquet (java/tparquet), not of parquet-java: the one way to give a ParquetWriter its
 * own ParquetProperties (and with them a ValuesWriterFactory) is its package-private
 * constructor. Used by io.aperiodic.tparquet.TparquetWriter.
 */
public final class TparquetWriters {
  private TparquetWriters() {}

  public static <T> ParquetWriter<T> create(OutputFile file, WriteSupport<T> writeSupport, CompressionCodecName codec,
      long rowGroupSize, ParquetConfiguration conf, ParquetProperties props) throws IOException {
    return new ParquetWriter<>(file, ParquetFileWriter.Mode.CREATE, writeSupport, codec, new CodecFactory(conf, props.getPageSizeThreshold()),
        rowGroupSize, false, conf, ParquetWriter.MAX_PADDING_SIZE_DEFAULT, props, null);
  }
}
