package org.apache.parquet.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.jupiter.api.Test;

/** The tparquet fork: PCODEC in the thrift enum (spec/FORMAT.md section 2). */
class TestPcodecCompressionCodec {
  @Test
  void theCodecIdOfSpecSection2() {
    assertThat(CompressionCodec.PCODEC.getValue()).isEqualTo(20547);
    assertThat(CompressionCodec.findByValue(20547)).isSameAs(CompressionCodec.PCODEC);
  }

  @Test
  void theStandardCodecsKeepTheirValues() {
    CompressionCodec[] standard = {
      CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP, CompressionCodec.LZO,
      CompressionCodec.BROTLI, CompressionCodec.LZ4, CompressionCodec.ZSTD, CompressionCodec.LZ4_RAW
    };
    for (int i = 0; i < standard.length; i++) {
      assertThat(standard[i].getValue()).isEqualTo(i);
      assertThat(CompressionCodec.findByValue(i)).isSameAs(standard[i]);
    }
    assertThat(CompressionCodec.values()).hasSize(standard.length + 1);
  }

  @Test
  void otherValuesAreStillUnknown() {
    for (int v : new int[] {-1, 8, 20546, 20548, Integer.MAX_VALUE}) {
      assertThat(CompressionCodec.findByValue(v)).isNull();
    }
  }

  @Test
  void aPcodecColumnChunkRoundTripsThroughThrift() throws Exception {
    ColumnMetaData md = new ColumnMetaData(
        Type.INT64, List.of(Encoding.PLAIN), List.of("ts"), CompressionCodec.PCODEC, 10, 100, 50, 4);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    md.write(new TCompactProtocol(new TIOStreamTransport(out)));
    ColumnMetaData back = new ColumnMetaData();
    back.read(new TCompactProtocol(new TIOStreamTransport(new ByteArrayInputStream(out.toByteArray()))));
    back.validate();
    assertThat(back.getCodec()).isSameAs(CompressionCodec.PCODEC);
    assertThat(back).isEqualTo(md);
  }

  @Test
  void theThriftDefinitionDeclaresIt() throws Exception {
    try (InputStream in = getClass().getResourceAsStream("/parquet.thrift")) {
      String thrift = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(thrift).contains("  PCODEC = 20547;");
    }
  }
}
