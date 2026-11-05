package org.apache.parquet.hadoop.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;

/** The tparquet fork: CompressionCodecName.PCODEC. */
class TestPcodecCompressionCodecName {
  @Test
  void pcodec() {
    CompressionCodecName c = CompressionCodecName.PCODEC;
    assertThat(c.getParquetCompressionCodec()).isSameAs(CompressionCodec.PCODEC);
    assertThat(c.getExtension()).isEqualTo(".pcodec");
    assertThat(c.getHadoopCompressionCodecClassName()).isEqualTo("org.apache.parquet.hadoop.codec.PcodecCodec");
  }

  @Test
  void fromTheConfigurationAndTheFooter() {
    assertThat(CompressionCodecName.fromConf("PCODEC")).isSameAs(CompressionCodecName.PCODEC);
    assertThat(CompressionCodecName.fromConf("pcodec")).isSameAs(CompressionCodecName.PCODEC);
    assertThat(CompressionCodecName.fromParquet(CompressionCodec.PCODEC)).isSameAs(CompressionCodecName.PCODEC);
    assertThat(CompressionCodecName.valueOf(CompressionCodec.PCODEC.name())).isSameAs(CompressionCodecName.PCODEC);
  }

  @Test
  void everyCodecMapsToTheFooterAndBack() {
    for (CompressionCodecName c : CompressionCodecName.values()) {
      assertThat(CompressionCodecName.fromParquet(c.getParquetCompressionCodec())).isSameAs(c);
    }
    assertThat(CompressionCodecName.values()).hasSize(CompressionCodec.values().length);
  }
}
