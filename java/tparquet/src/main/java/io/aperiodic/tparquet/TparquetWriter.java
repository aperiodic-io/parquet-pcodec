package io.aperiodic.tparquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.TparquetWriters;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.codec.PcodecCodec;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

/**
 * Writes Parquet files with PCODEC the way spec/FORMAT.md section 3 recommends: DATA_PAGE_V2
 * pages, and for the INT32, INT64, FLOAT and DOUBLE columns PLAIN encoding without a dictionary
 * (floats bit for bit). The other columns keep parquet-java's encodings (dictionaries for
 * strings); parquet-java has one codec per file, so their pages are the payload's ZSTD fallback.
 */
public final class TparquetWriter {
  private TparquetWriter() {}

  /** Writer settings; {@link #defaults()} is PCODEC level 8 with DECIMAL, 128 MiB row groups, 1 MiB pages. */
  public static final class Options implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    final CompressionCodecName codec;
    final int level;
    final boolean decimal;
    final long rowGroupSize;
    final int pageSize;
    final HashMap<String, String> extraMetadata;

    private Options(CompressionCodecName codec, int level, boolean decimal, long rowGroupSize, int pageSize, Map<String, String> extra) {
      this.codec = Objects.requireNonNull(codec);
      if (level < 0 || level > Pcodec.MAX_LEVEL) {
        throw new IllegalArgumentException("level " + level + " outside 0.." + Pcodec.MAX_LEVEL);
      }
      this.level = level;
      this.decimal = decimal;
      this.rowGroupSize = rowGroupSize;
      this.pageSize = pageSize;
      this.extraMetadata = new HashMap<>(extra);
    }

    public static Options defaults() {
      return new Options(CompressionCodecName.PCODEC, Pcodec.DEFAULT_LEVEL, true, 128L << 20, 1 << 20, Map.of());
    }

    public Options withCodec(CompressionCodecName codec) {
      return new Options(codec, level, decimal, rowGroupSize, pageSize, extraMetadata);
    }

    public Options withLevel(int level) {
      return new Options(codec, level, decimal, rowGroupSize, pageSize, extraMetadata);
    }

    public Options withDecimal(boolean decimal) {
      return new Options(codec, level, decimal, rowGroupSize, pageSize, extraMetadata);
    }

    public Options withRowGroupSize(long bytes) {
      return new Options(codec, level, decimal, bytes, pageSize, extraMetadata);
    }

    public Options withPageSize(int bytes) {
      return new Options(codec, level, decimal, rowGroupSize, bytes, extraMetadata);
    }

    public Options withExtraMetadata(Map<String, String> kv) {
      return new Options(codec, level, decimal, rowGroupSize, pageSize, kv);
    }
  }

  /** A writer of {@code schema}'s rows to {@code file} (replaced if it exists). */
  public static ParquetWriter<Group> groups(Path file, MessageType schema, Options options) throws IOException {
    Files.deleteIfExists(file);
    PlainParquetConfiguration conf = new PlainParquetConfiguration();
    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
    return create(new LocalOutputFile(file), new GroupWriteSupport(), schema, conf, options);
  }

  /** A writer for any write support whose schema is {@code schema}; {@code conf} gets the PCODEC level and decimal flag. */
  public static <T> ParquetWriter<T> create(
      OutputFile file, WriteSupport<T> writeSupport, MessageType schema, ParquetConfiguration conf, Options options)
      throws IOException {
    conf.setInt(PcodecCodec.LEVEL, options.level);
    conf.setBoolean(PcodecCodec.DECIMAL, options.decimal);
    ParquetProperties.Builder props = ParquetProperties.builder()
        .withWriterVersion(WriterVersion.PARQUET_2_0)
        .withPageSize(options.pageSize)
        .withDictionaryPageSize(options.pageSize)
        .withValuesWriterFactory(new PlainNumbersValuesWriterFactory())
        .withExtraMetaData(new HashMap<>(options.extraMetadata));
    for (ColumnDescriptor c : schema.getColumns()) {
      if (PlainNumbersValuesWriterFactory.isNumber(c)) {
        props.withDictionaryEncoding(String.join(".", c.getPath()), false);
      }
    }
    return TparquetWriters.create(file, writeSupport, options.codec, options.rowGroupSize, conf, props.build());
  }
}
