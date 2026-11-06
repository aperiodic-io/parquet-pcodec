package io.aperiodic.tparquet;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;

/** Reads the rows of a Parquet file (PCODEC or not) as parquet-java {@link Group}s. */
public final class TparquetReader implements Closeable {
  private final ParquetReader<Group> reader;

  private TparquetReader(ParquetReader<Group> reader) {
    this.reader = reader;
  }

  public static TparquetReader open(Path file) throws IOException {
    return new TparquetReader(new GroupReaderBuilder(new LocalInputFile(file)).build());
  }

  private static final class GroupReaderBuilder extends ParquetReader.Builder<Group> {
    GroupReaderBuilder(LocalInputFile file) {
      super(file);
    }

    @Override
    protected ReadSupport<Group> getReadSupport() {
      return new GroupReadSupport();
    }
  }

  /** The next row, or {@code null} after the last. */
  public Group read() throws IOException {
    return reader.read();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
