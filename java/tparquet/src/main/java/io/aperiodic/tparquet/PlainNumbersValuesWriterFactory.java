package io.aperiodic.tparquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.LittleEndianDataOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory;
import org.apache.parquet.column.values.factory.ValuesWriterFactory;

/**
 * PLAIN values for the columns PCODEC models (spec section 3, writer recommendations), whatever
 * the writer version: parquet-java's V2 writer would encode integers DELTA_BINARY_PACKED. Floats
 * are written bit for bit, NaN payloads included. Other columns get parquet-java's defaults.
 */
final class PlainNumbersValuesWriterFactory implements ValuesWriterFactory {
  private final DefaultValuesWriterFactory defaults = new DefaultValuesWriterFactory();
  private ParquetProperties props;

  static boolean isNumber(ColumnDescriptor c) {
    return PhysicalType.fromName(c.getPrimitiveType().getPrimitiveTypeName().name()) != null;
  }

  @Override
  public void initialize(ParquetProperties properties) {
    props = properties;
    defaults.initialize(properties);
  }

  @Override
  public ValuesWriter newValuesWriter(ColumnDescriptor descriptor) {
    if (isNumber(descriptor)) {
      return new PlainNumbers(props.getInitialSlabSize(), props.getPageSizeThreshold(), props);
    }
    return defaults.newValuesWriter(descriptor);
  }

  /** Little-endian PLAIN numbers; doubles and floats as their raw bits. */
  static final class PlainNumbers extends ValuesWriter {
    private final CapacityByteArrayOutputStream bytes;
    private final LittleEndianDataOutputStream out;

    PlainNumbers(int initialSize, int pageSize, ParquetProperties props) {
      bytes = new CapacityByteArrayOutputStream(initialSize, pageSize, props.getAllocator());
      out = new LittleEndianDataOutputStream(bytes);
    }

    @Override
    public void writeInteger(int v) {
      try {
        out.writeInt(v);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void writeLong(long v) {
      try {
        out.writeLong(v);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void writeFloat(float v) {
      writeInteger(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v) {
      writeLong(Double.doubleToRawLongBits(v));
    }

    @Override
    public long getBufferedSize() {
      return bytes.size();
    }

    @Override
    public BytesInput getBytes() {
      try {
        out.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return BytesInput.from(bytes);
    }

    @Override
    public Encoding getEncoding() {
      return Encoding.PLAIN;
    }

    @Override
    public void reset() {
      bytes.reset();
    }

    @Override
    public void close() {
      bytes.close();
    }

    @Override
    public long getAllocatedSize() {
      return bytes.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
      return bytes.memUsageString(prefix + " PLAIN numbers");
    }
  }
}
