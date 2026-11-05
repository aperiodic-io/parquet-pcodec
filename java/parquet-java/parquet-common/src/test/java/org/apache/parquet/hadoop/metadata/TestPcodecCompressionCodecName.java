package org.apache.parquet.hadoop.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  @Test
  void fromTheHadoopCodecClass() {
    assertThat(CompressionCodecName.fromCompressionCodec(org.apache.parquet.hadoop.codec.PcodecCodec.class))
        .isSameAs(CompressionCodecName.PCODEC);
    assertThat(CompressionCodecName.fromCompressionCodec(null)).isSameAs(CompressionCodecName.UNCOMPRESSED);
    assertThatThrownBy(() -> CompressionCodecName.fromCompressionCodec(String.class))
        .isInstanceOf(org.apache.parquet.hadoop.codec.CompressionCodecNotSupportedException.class);
  }

  @Test
  void theHadoopCodecClassIsLoadedWhenPresent() {
    assertThat(CompressionCodecName.PCODEC.getHadoopCompressionCodecClass()).isSameAs(org.apache.parquet.hadoop.codec.PcodecCodec.class);
    // no class (UNCOMPRESSED), and a class that is not on the class path (LZO)
    assertThat(CompressionCodecName.UNCOMPRESSED.getHadoopCompressionCodecClass()).isNull();
    assertThat(CompressionCodecName.LZO.getHadoopCompressionCodecClass()).isNull();
  }

  @Test
  void noConfiguredCodecIsUncompressed() {
    assertThat(CompressionCodecName.fromConf(null)).isSameAs(CompressionCodecName.UNCOMPRESSED);
  }
}
