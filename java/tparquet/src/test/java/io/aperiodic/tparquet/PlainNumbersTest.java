package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.junit.jupiter.api.Test;

/** The PLAIN writer of the numeric columns, alone. */
class PlainNumbersTest {
  static PlainNumbersValuesWriterFactory.PlainNumbers writer() {
    return new PlainNumbersValuesWriterFactory.PlainNumbers(64, 1 << 16, ParquetProperties.builder().build());
  }

  @Test
  void littleEndianValuesAndRawFloatBits() throws Exception {
    var w = writer();
    w.writeInteger(-2);
    w.writeLong(1L << 40);
    w.writeFloat(Float.intBitsToFloat(0x7fc00001));
    w.writeDouble(Double.longBitsToDouble(0xfff800000000beefL));
    assertThat(w.getBufferedSize()).isEqualTo(24);
    assertThat(w.getAllocatedSize()).isGreaterThanOrEqualTo(24);
    assertThat(w.getEncoding()).isSameAs(Encoding.PLAIN);
    ByteBuffer b = ByteBuffer.wrap(w.getBytes().toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
    assertThat(b.getInt()).isEqualTo(-2);
    assertThat(b.getLong()).isEqualTo(1L << 40);
    assertThat(b.getInt()).isEqualTo(0x7fc00001);
    assertThat(b.getLong()).isEqualTo(0xfff800000000beefL);
    assertThat(w.memUsageString("col")).contains("PLAIN numbers");
    w.reset();
    assertThat(w.getBufferedSize()).isZero();
  }
}
