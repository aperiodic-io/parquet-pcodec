package org.apache.parquet.hadoop.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

/** The Hadoop codec class of CompressionCodecName.PCODEC, and the footer key (spec section 2). */
class TestPcodecCodec {
  @Test
  void theCodecNameNamesThisClass() {
    assertThat(CompressionCodecName.PCODEC.getHadoopCompressionCodecClass()).isSameAs(PcodecCodec.class);
    assertThat(CompressionCodecName.fromCompressionCodec(PcodecCodec.class)).isSameAs(CompressionCodecName.PCODEC);
  }

  @Test
  void streamsAreRefusedNotSilentlyWrong() {
    PcodecCodec c = new PcodecCodec();
    String msg = "PCODEC needs each page's type and expected size: use CodecFactory, not the Hadoop stream API";
    assertThatThrownBy(() -> c.createOutputStream(new ByteArrayOutputStream()))
        .isInstanceOf(UnsupportedOperationException.class).hasMessage(msg);
    assertThatThrownBy(() -> c.createOutputStream(new ByteArrayOutputStream(), null)).hasMessage(msg);
    assertThatThrownBy(() -> c.createInputStream(new ByteArrayInputStream(new byte[0]))).hasMessage(msg);
    assertThatThrownBy(() -> c.createInputStream(new ByteArrayInputStream(new byte[0]), null)).hasMessage(msg);
    assertThatThrownBy(c::createCompressor).hasMessage(msg);
    assertThatThrownBy(c::createDecompressor).hasMessage(msg);
    assertThatThrownBy(c::getCompressorType).hasMessage(msg);
    assertThatThrownBy(c::getDecompressorType).hasMessage(msg);
    assertThat(c.getDefaultExtension()).isEqualTo(".pcodec");
  }

  @Test
  void configurable() {
    PcodecCodec c = new PcodecCodec();
    assertThat(c.getConf()).isNull();
    Configuration conf = new Configuration(false);
    c.setConf(conf);
    assertThat(c.getConf()).isSameAs(conf);
  }

  static BlockMetaData block(CompressionCodecName... codecs) {
    BlockMetaData b = new BlockMetaData();
    int i = 0;
    for (CompressionCodecName codec : codecs) {
      Set<Encoding> enc = new HashSet<>(List.of(Encoding.PLAIN));
      b.addColumn(ColumnChunkMetaData.get(ColumnPath.get("c" + i++), PrimitiveTypeName.INT64, codec, enc,
          Statistics.createStats(org.apache.parquet.schema.Types.required(PrimitiveTypeName.INT64).named("c")), 4, 0, 10, 100, 50));
    }
    return b;
  }

  @Test
  void theFooterKeyIsAddedWhenAChunkUsesPcodec() {
    Map<String, String> kv = Map.of("a", "b");
    assertThat(PcodecCodec.withVersion(kv, List.of(block(CompressionCodecName.ZSTD), block(CompressionCodecName.PCODEC))))
        .containsExactlyInAnyOrderEntriesOf(Map.of("a", "b", "parquet.pcodec.version", "1"));
    assertThat(PcodecCodec.withVersion(kv, List.of(block(CompressionCodecName.ZSTD, CompressionCodecName.PCODEC))))
        .containsEntry("parquet.pcodec.version", "1");
    // the input is not changed
    assertThat(kv).containsOnlyKeys("a");
  }

  @Test
  void theFooterKeyIsNotAddedWithoutPcodec() {
    Map<String, String> kv = Map.of("a", "b");
    assertThat(PcodecCodec.withVersion(kv, List.of(block(CompressionCodecName.ZSTD, CompressionCodecName.SNAPPY))))
        .isSameAs(kv);
    assertThat(PcodecCodec.withVersion(kv, List.of())).isSameAs(kv);
    assertThat(PcodecCodec.withVersion(kv, List.of(new BlockMetaData()))).isSameAs(kv);
  }

  @Test
  void aKeyAlreadyThereIsKept() {
    Map<String, String> kv = Map.of("parquet.pcodec.version", "custom");
    assertThat(PcodecCodec.withVersion(kv, List.of(block(CompressionCodecName.PCODEC)))).isSameAs(kv);
  }

  @Test
  void configurationKeys() {
    assertThat(PcodecCodec.LEVEL).isEqualTo("parquet.compression.codec.pcodec.level");
    assertThat(PcodecCodec.DECIMAL).isEqualTo("parquet.compression.codec.pcodec.decimal");
    assertThat(PcodecCodec.DEFAULT_LEVEL).isEqualTo(8);
  }
}
