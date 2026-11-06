package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.aperiodic.tparquet.Mode;
import io.aperiodic.tparquet.Pcodec;
import io.aperiodic.tparquet.PcodecException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.codec.PcodecCodec;
import org.apache.parquet.hadoop.codec.PcodecCompressor;
import org.apache.parquet.hadoop.codec.PcodecDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** CodecFactory and DirectCodecFactory create PCODEC compressors and decompressors (FORK.md). */
class TestPcodecCodecFactory {
  static byte[] doubles(int n) {
    ByteBuffer b = ByteBuffer.allocate(8 * n).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < n; i++) {
      b.putDouble((100_000 + i % 977) / 100.0);
    }
    return b.array();
  }

  static byte[] floats(int n) {
    ByteBuffer b = ByteBuffer.allocate(4 * n).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < n; i++) {
      b.putFloat((10_000 + i % 500) / 100f);
    }
    return b.array();
  }

  static CodecFactory heap(Configuration conf) {
    return new CodecFactory(conf, 1 << 20);
  }

  static CodecFactory direct(Configuration conf) {
    return CodecFactory.createDirectCodecFactory(conf, new DirectByteBufferAllocator(), 1 << 20);
  }

  static ColumnDescriptor column(PrimitiveTypeName t) {
    return new ColumnDescriptor(new String[] {"c"}, Types.required(t).length(t == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY ? 4 : 0).named("c"), 0, 0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void bothFactoriesCreatePcodecCodecs(boolean isDirect) throws IOException {
    CodecFactory f = isDirect ? direct(new Configuration()) : heap(new Configuration());
    BytesInputCompressor c = f.getCompressor(CompressionCodecName.PCODEC);
    BytesInputDecompressor d = f.getDecompressor(CompressionCodecName.PCODEC);
    assertThat(c).isInstanceOf(PcodecCompressor.class);
    assertThat(d).isInstanceOf(PcodecDecompressor.class);
    assertThat(c.getCodecName()).isSameAs(CompressionCodecName.PCODEC);
    byte[] src = doubles(1000);
    BytesInput p = c.compress(BytesInput.from(src));
    assertThat(d.decompress(p, src.length).toByteArray()).isEqualTo(src);
    // cached, and released without error
    assertThat(f.getCompressor(CompressionCodecName.PCODEC)).isSameAs(c);
    assertThat(f.getDecompressor(CompressionCodecName.PCODEC)).isSameAs(d);
    f.release();
  }

  @Test
  void theOtherCodecsAreUnchanged() {
    CodecFactory f = heap(new Configuration());
    assertThat(f.getCompressor(CompressionCodecName.ZSTD)).isNotInstanceOf(PcodecCompressor.class);
    assertThat(f.getDecompressor(CompressionCodecName.SNAPPY)).isNotInstanceOf(PcodecDecompressor.class);
    assertThat(f.getCompressor(CompressionCodecName.UNCOMPRESSED)).isSameAs(CodecFactory.NO_OP_COMPRESSOR);
    CodecFactory d = direct(new Configuration());
    assertThat(d.getDecompressor(CompressionCodecName.ZSTD)).isNotInstanceOf(PcodecDecompressor.class);
  }

  /** Without a column the compressor has no hint: numbers go to the ZSTD fallback. */
  @Test
  void theFactoryCompressorHasNoHint() throws IOException {
    PcodecCompressor c = (PcodecCompressor) heap(new Configuration()).getCompressor(CompressionCodecName.PCODEC);
    assertThat(c.hint()).isNull();
    assertThat(Pcodec.mode(c.compress(BytesInput.from(doubles(1000))).toByteArray())).isEqualTo(Mode.ZSTD);
  }

  @ParameterizedTest
  @EnumSource(PrimitiveTypeName.class)
  void eachColumnGetsItsTypeAsTheHint(PrimitiveTypeName t) {
    PcodecCompressor c = (PcodecCompressor) heap(new Configuration()).getCompressor(CompressionCodecName.PCODEC);
    BytesInputCompressor typed = PcodecCompressor.forColumn(c, column(t));
    assertThat(typed).isInstanceOf(PcodecCompressor.class);
    String hint = ((PcodecCompressor) typed).hint() == null ? null : ((PcodecCompressor) typed).hint().name();
    switch (t) {
      case INT32, INT64, FLOAT, DOUBLE -> assertThat(hint).isEqualTo(t.name());
      default -> assertThat(hint).isNull();
    }
    assertThat(typed.getCodecName()).isSameAs(CompressionCodecName.PCODEC);
  }

  @Test
  void otherCompressorsAreNotWrapped() {
    BytesInputCompressor zstd = heap(new Configuration()).getCompressor(CompressionCodecName.ZSTD);
    assertThat(PcodecCompressor.forColumn(zstd, column(PrimitiveTypeName.INT64))).isSameAs(zstd);
  }

  @Test
  void theTypedCompressorWritesTheHintedModes() throws IOException {
    PcodecCompressor c = (PcodecCompressor) heap(new Configuration()).getCompressor(CompressionCodecName.PCODEC);
    BytesInput p = PcodecCompressor.forColumn(c, column(PrimitiveTypeName.DOUBLE)).compress(BytesInput.from(doubles(1000)));
    assertThat(Pcodec.mode(p.toByteArray())).isEqualTo(Mode.DECIMAL);
  }

  @Test
  void theLevelAndDecimalFlagComeFromTheConfiguration() throws IOException {
    Configuration conf = new Configuration();
    conf.setInt(PcodecCodec.LEVEL, 1);
    conf.setBoolean(PcodecCodec.DECIMAL, false);
    PcodecCompressor c = (PcodecCompressor) heap(conf).getCompressor(CompressionCodecName.PCODEC);
    assertThat(c.level()).isEqualTo(1);
    assertThat(c.decimal()).isFalse();
    byte[] src = floats(10_000);
    PcodecCompressor typed = (PcodecCompressor) PcodecCompressor.forColumn(c, column(PrimitiveTypeName.FLOAT));
    assertThat(typed.level()).isEqualTo(1);
    assertThat(typed.decimal()).isFalse();
    byte[] p = typed.compress(BytesInput.from(src)).toByteArray();
    assertThat(p).isEqualTo(Pcodec.compress(src, 0, src.length, io.aperiodic.tparquet.PhysicalType.FLOAT, 1, false));

    PcodecCompressor defaults = (PcodecCompressor) heap(new Configuration()).getCompressor(CompressionCodecName.PCODEC);
    assertThat(defaults.level()).isEqualTo(8);
    assertThat(defaults.decimal()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 12})
  void theLowestAndHighestLevelsAreAccepted(int level) {
    Configuration conf = new Configuration();
    conf.setInt(PcodecCodec.LEVEL, level);
    assertThat(((PcodecCompressor) heap(conf).getCompressor(CompressionCodecName.PCODEC)).level()).isEqualTo(level);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 13})
  void aLevelOutOfRangeIsRefusedWhenTheCompressorIsCreated(int level) {
    Configuration conf = new Configuration();
    conf.setInt(PcodecCodec.LEVEL, level);
    assertThatThrownBy(() -> heap(conf).getCompressor(CompressionCodecName.PCODEC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("parquet.compression.codec.pcodec.level=" + level + ": the level is 0..12");
  }

  @Test
  void aCorruptPayloadIsAnIOException() {
    BytesInputDecompressor d = heap(new Configuration()).getDecompressor(CompressionCodecName.PCODEC);
    assertThatThrownBy(() -> d.decompress(BytesInput.from(new byte[] {9, 0}), 0 + 1))
        .isInstanceOf(PcodecException.class).hasMessage("pcodec payload: unsupported version 9");
  }

  /** As the reference reader (rust/parquet decode_page), an empty values section is not decoded. */
  @Test
  void anEmptyValuesSectionIsNotDecoded() throws IOException {
    PcodecDecompressor d = new PcodecDecompressor();
    assertThat(d.decompress(BytesInput.empty(), 0).size()).isZero();
    assertThat(d.decompress(BytesInput.from(new byte[] {1, 0}), 0).size()).isZero();
    ByteBuffer out = ByteBuffer.allocateDirect(4);
    ByteBuffer in = ByteBuffer.allocateDirect(0);
    d.decompress(in, 0, out, 0);
    assertThat(out.position()).isZero();
    // a payload that is there all the same is skipped: the input moves past it
    ByteBuffer raw = ByteBuffer.allocateDirect(5).put(new byte[] {7, 1, 0, 7, 7}).flip().position(1);
    d.decompress(raw, 2, out, 0);
    assertThat(raw.position()).isEqualTo(3);
    assertThat(out.position()).isZero();
  }

  /** The ByteBuffer method reads compressedSize bytes at the input's position, as the other codecs do. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void byteBuffers(boolean isDirect) throws IOException {
    byte[] src = doubles(500);
    byte[] payload = Pcodec.compress(src, io.aperiodic.tparquet.PhysicalType.DOUBLE);
    ByteBuffer in = isDirect ? ByteBuffer.allocateDirect(payload.length + 10) : ByteBuffer.allocate(payload.length + 10);
    in.position(3);
    in.put(payload);
    in.put(new byte[] {7, 7, 7});
    in.flip().position(3);
    ByteBuffer out = isDirect ? ByteBuffer.allocateDirect(src.length + 2) : ByteBuffer.allocate(src.length + 2);
    out.position(1);
    BytesInputDecompressor d = direct(new Configuration()).getDecompressor(CompressionCodecName.PCODEC);
    d.decompress(in, payload.length, out, src.length);
    assertThat(in.position()).isEqualTo(3 + payload.length);
    assertThat(in.limit()).isEqualTo(3 + payload.length + 3);
    assertThat(out.position()).isEqualTo(1 + src.length);
    byte[] got = new byte[src.length];
    out.flip().position(1);
    out.get(got);
    assertThat(got).isEqualTo(src);
  }

  @Test
  void aShortInputBufferIsRefused() {
    BytesInputDecompressor d = new PcodecDecompressor();
    assertThatThrownBy(() -> d.decompress(ByteBuffer.allocate(4), 5, ByteBuffer.allocate(8), 8))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("input has 4 bytes, the page 5");
  }

  @Test
  void aCorruptDirectPayloadLeavesTheInput() {
    ByteBuffer in = ByteBuffer.allocateDirect(2).put(new byte[] {1, 9}).flip();
    assertThatThrownBy(() -> new PcodecDecompressor().decompress(in, 2, ByteBuffer.allocateDirect(8), 8))
        .isInstanceOf(PcodecException.class).hasMessage("pcodec payload: unknown mode 9");
    assertThat(in.position()).isZero();
  }
}
