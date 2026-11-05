package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.aperiodic.tparquet.PcodecException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageType;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Files written and read through the fork with PCODEC: rows, footer, pages, failures. */
class TestPcodecFiles {
  static final MessageType SCHEMA = MessageTypeParser.parseMessageType(String.join("\n",
      "message trades {",
      "  required int32 i32;",
      "  required int32 lot;",
      "  optional int64 i64;",
      "  required float f32;",
      "  optional double f64;",
      "  required double price;",
      "  required int64 ts (TIMESTAMP(MILLIS,true));",
      "  required binary sym (STRING);",
      "  optional boolean buy;",
      "  optional group levels (LIST) { repeated group list { optional int64 element; } }",
      "}"));

  @TempDir java.nio.file.Path dir;

  static List<Group> rows(int n) {
    SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
    List<Group> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Group g = f.newGroup()
          .append("i32", 1000 + 3 * i)
          .append("lot", (i * 7 % 200) * 100)
          .append("f32", (10_000 + i % 500) / 100f)
          .append("price", (100_000 + (i * 7919) % 977) / 100.0)
          .append("ts", 1_735_689_600_000L + 37L * i)
          .append("sym", new String[] {"BTC-USD", "ETH-USD", "SOL-USD"}[i % 3]);
      if (i % 5 != 0) {
        g.append("i64", 1L << 40 | i);
      }
      if (i % 7 != 0) {
        g.append("f64", i * 0.25);
      }
      if (i % 3 != 0) {
        g.append("buy", i % 2 == 0);
      }
      if (i % 4 != 0) {
        Group levels = g.addGroup("levels");
        for (int j = 0; j < i % 6; j++) {
          Group e = levels.addGroup("list");
          if (j != 2) {
            e.append("element", (long) i * 10 + j);
          }
        }
      }
      out.add(g);
    }
    return out;
  }

  java.nio.file.Path write(List<Group> rows, CompressionCodecName codec, WriterVersion version, boolean dictionary,
      int rowGroupSize, Map<String, String> extra) throws IOException {
    java.nio.file.Path file = dir.resolve("f-" + System.nanoTime() + ".parquet");
    try (ParquetWriter<Group> w = ExampleParquetWriter.builder(new Path(file.toUri()))
        .withType(SCHEMA)
        .withConf(new Configuration())
        .withCompressionCodec(codec)
        .withWriterVersion(version)
        .withDictionaryEncoding(dictionary)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(4096)
        .withExtraMetaData(extra)
        .build()) {
      for (Group g : rows) {
        w.write(g);
      }
    }
    return file;
  }

  java.nio.file.Path write(List<Group> rows, WriterVersion version, boolean dictionary) throws IOException {
    return write(rows, CompressionCodecName.PCODEC, version, dictionary, 16 * 1024, Map.of());
  }

  /** Rewrites a file in place, without the checksum file Hadoop's LocalFileSystem keeps (it would catch the change first). */
  static void tamper(java.nio.file.Path file, byte[] bytes) throws IOException {
    Files.write(file, bytes);
    Files.deleteIfExists(file.resolveSibling("." + file.getFileName() + ".crc"));
  }

  static List<String> read(java.nio.file.Path file, Configuration conf, boolean direct) throws IOException {
    ParquetReader.Builder<Group> b = ParquetReader.builder(new GroupReadSupport(), new Path(file.toUri())).withConf(conf);
    if (direct) {
      DirectByteBufferAllocator alloc = new DirectByteBufferAllocator();
      b = b.withAllocator(alloc).withCodecFactory(CodecFactory.createDirectCodecFactory(conf, alloc, 1 << 20));
    }
    List<String> out = new ArrayList<>();
    try (ParquetReader<Group> r = b.build()) {
      for (Group g = r.read(); g != null; g = r.read()) {
        out.add(g.toString());
      }
    }
    return out;
  }

  static List<String> read(java.nio.file.Path file) throws IOException {
    return read(file, new Configuration(), false);
  }

  static ParquetMetadata footer(java.nio.file.Path file) throws IOException {
    try (ParquetFileReader r = ParquetFileReader.open(new Configuration(), new Path(file.toUri()))) {
      return r.getFooter();
    }
  }

  static Stream<Arguments> layouts() {
    return Stream.of(
        Arguments.of(WriterVersion.PARQUET_1_0, false),
        Arguments.of(WriterVersion.PARQUET_1_0, true),
        Arguments.of(WriterVersion.PARQUET_2_0, false),
        Arguments.of(WriterVersion.PARQUET_2_0, true));
  }

  @ParameterizedTest(name = "{0} dictionary {1}")
  @MethodSource("layouts")
  void rowsRoundTripInEveryPageLayout(WriterVersion version, boolean dictionary) throws IOException {
    List<Group> rows = rows(20_000);
    java.nio.file.Path file = write(rows, version, dictionary);
    List<String> want = rows.stream().map(Group::toString).toList();
    assertThat(read(file)).isEqualTo(want);
    ParquetMetadata footer = footer(file);
    assertThat(footer.getBlocks().size()).as("row groups").isGreaterThan(1);
    for (BlockMetaData b : footer.getBlocks()) {
      for (ColumnChunkMetaData c : b.getColumns()) {
        assertThat(c.getCodec()).as(c.getPath().toDotString()).isSameAs(CompressionCodecName.PCODEC);
      }
    }
    assertThat(footer.getFileMetaData().getKeyValueMetaData()).containsEntry("parquet.pcodec.version", "1");
  }

  @ParameterizedTest(name = "{0} dictionary {1}")
  @MethodSource("layouts")
  void theDirectBufferPathReadsTheSameRows(WriterVersion version, boolean dictionary) throws IOException {
    List<Group> rows = rows(5000);
    java.nio.file.Path file = write(rows, version, dictionary);
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED, true);
    assertThat(read(file, conf, true)).isEqualTo(rows.stream().map(Group::toString).toList());
  }

  /** The column's type reaches the encoder: required PLAIN columns in V1 pages are pcodec's own modes. */
  @Test
  void eachColumnIsEncodedForItsType() throws IOException {
    Map<String, List<String>> modes = PcodecPages.modes(write(rows(20_000), WriterVersion.PARQUET_1_0, false));
    assertThat(modes.get("i32")).as(modes.toString()).containsOnly("DATA_PAGE:PCO");
    assertThat(modes.get("ts")).containsOnly("DATA_PAGE:PCO");
    // floats: DECIMAL, or PCO where that is smaller (spec section 5); never the untyped fallback
    assertThat(modes.get("f32")).isSubsetOf("DATA_PAGE:DECIMAL", "DATA_PAGE:PCO").contains("DATA_PAGE:DECIMAL");
    assertThat(modes.get("price")).as(modes.toString()).isSubsetOf("DATA_PAGE:DECIMAL", "DATA_PAGE:PCO").contains("DATA_PAGE:DECIMAL");
    // V1 pages of optional columns start with the levels: not whole values, so the ZSTD fallback
    assertThat(modes.get("i64")).isSubsetOf("DATA_PAGE:ZSTD", "DATA_PAGE:RAW");
    // no hint for strings and booleans: the ZSTD fallback, or RAW when that is not smaller
    assertThat(modes.get("sym")).isSubsetOf("DATA_PAGE:ZSTD", "DATA_PAGE:RAW");
    assertThat(modes.get("buy")).isSubsetOf("DATA_PAGE:ZSTD", "DATA_PAGE:RAW");
  }

  /**
   * V2 pages keep the levels out of the payload (spec section 3), but parquet-java's V2 writer
   * encodes integers DELTA_BINARY_PACKED unless told otherwise: not PLAIN values, so the ZSTD
   * fallback (the tparquet writer asks for PLAIN). Floats stay PLAIN and get DECIMAL.
   */
  @Test
  void v2PagesWithParquetJavasDefaultEncodings() throws IOException {
    Map<String, List<String>> modes = PcodecPages.modes(write(rows(20_000), WriterVersion.PARQUET_2_0, false));
    assertThat(modes.get("f32")).isSubsetOf("DATA_PAGE_V2:DECIMAL", "DATA_PAGE_V2:PCO").contains("DATA_PAGE_V2:DECIMAL");
    assertThat(modes.get("price")).as(modes.toString()).isSubsetOf("DATA_PAGE_V2:DECIMAL", "DATA_PAGE_V2:PCO").contains("DATA_PAGE_V2:DECIMAL");
    // multiples of 0.25: whichever of PCO and DECIMAL is smaller, page by page
    assertThat(modes.get("f64")).isSubsetOf("DATA_PAGE_V2:DECIMAL", "DATA_PAGE_V2:PCO");
    assertThat(modes.get("i32")).isSubsetOf("DATA_PAGE_V2:ZSTD", "DATA_PAGE_V2:RAW");
  }

  /** Dictionary pages hold PLAIN values of the column's type and get its hint too. */
  @Test
  void dictionaryPagesGetTheHint() throws IOException {
    java.nio.file.Path file = write(rows(20_000), CompressionCodecName.PCODEC, WriterVersion.PARQUET_1_0, true, 1 << 20, Map.of());
    List<String> dict = PcodecPages.pages(file).stream()
        .filter(p -> p.column().equals("lot") && p.type() == PageType.DICTIONARY_PAGE)
        .map(PcodecPages.Page::mode).distinct().toList();
    assertThat(dict).containsOnly("PCO");
  }

  @Test
  void otherCodecsWriteNoFooterKey() throws IOException {
    java.nio.file.Path file = write(rows(100), CompressionCodecName.ZSTD, WriterVersion.PARQUET_2_0, false, 1 << 20, Map.of());
    assertThat(footer(file).getFileMetaData().getKeyValueMetaData()).doesNotContainKey("parquet.pcodec.version");
    assertThat(read(file)).hasSize(100);
  }

  @Test
  void theFooterKeyIsAddedNextToTheUsersKeys() throws IOException {
    java.nio.file.Path file = write(rows(100), CompressionCodecName.PCODEC, WriterVersion.PARQUET_2_0, false, 1 << 20,
        Map.of("writer", "test"));
    assertThat(footer(file).getFileMetaData().getKeyValueMetaData())
        .containsEntry("writer", "test").containsEntry("parquet.pcodec.version", "1");
  }

  @Test
  void anEmptyFileHasNoPcodecChunkAndNoKey() throws IOException {
    java.nio.file.Path file = write(List.of(), WriterVersion.PARQUET_2_0, false);
    assertThat(read(file)).isEmpty();
    assertThat(footer(file).getFileMetaData().getKeyValueMetaData()).doesNotContainKey("parquet.pcodec.version");
  }

  static long[] DOUBLE_BITS = {0x7ff8000000000001L, 0xfff0000000000000L, 0x8000000000000000L, 0L, 1L, 0x7ff0000000000000L, 0xfff8000000000000L};
  static int[] FLOAT_BITS = {0x7fc00001, 0xff800000, 0x80000000, 0, 1, 0x7f800000, 0xffc00000};

  java.nio.file.Path writeSpecials(CompressionCodecName codec) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required double d; required float f; }");
    java.nio.file.Path file = dir.resolve("specials-" + codec + ".parquet");
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> w = ExampleParquetWriter.builder(new Path(file.toUri())).withType(schema)
        .withConf(new Configuration()).withCompressionCodec(codec)
        .withWriterVersion(WriterVersion.PARQUET_2_0).withDictionaryEncoding(false).build()) {
      for (int i = 0; i < 700; i++) {
        w.write(f.newGroup().append("d", Double.longBitsToDouble(DOUBLE_BITS[i % 7]))
            .append("f", Float.intBitsToFloat(FLOAT_BITS[i % 7])));
      }
    }
    return file;
  }

  static List<String> bits(java.nio.file.Path file) throws IOException {
    List<String> out = new ArrayList<>();
    try (ParquetReader<Group> r = ParquetReader.builder(new GroupReadSupport(), new Path(file.toUri())).build()) {
      for (Group g = r.read(); g != null; g = r.read()) {
        out.add(Long.toHexString(Double.doubleToRawLongBits(g.getDouble("d", 0))) + " "
            + Integer.toHexString(Float.floatToRawIntBits(g.getFloat("f", 0))));
      }
    }
    return out;
  }

  /**
   * Signed zeros, infinities, subnormals and NaNs come back bit for bit, as without compression.
   * (parquet-java's PLAIN writer itself writes every NaN as the canonical NaN, doubleToLongBits;
   * reading NaN payloads written by others is checked by the conformance vectors.)
   */
  @Test
  void specialFloatsAsUncompressed() throws IOException {
    List<String> pcodec = bits(writeSpecials(CompressionCodecName.PCODEC));
    assertThat(pcodec).hasSize(700).isEqualTo(bits(writeSpecials(CompressionCodecName.UNCOMPRESSED)));
    assertThat(pcodec).contains("8000000000000000 80000000", "1 1", "fff0000000000000 ff800000");
  }

  /** A corrupt payload in a file is a decoding error, never a crash. */
  @Test
  void aCorruptPayloadInAFileIsAnError() throws IOException {
    java.nio.file.Path file = write(rows(2000), WriterVersion.PARQUET_2_0, false);
    PcodecPages.Page page = PcodecPages.pages(file).stream().filter(p -> p.column().equals("ts")).findFirst().orElseThrow();
    byte[] bytes = Files.readAllBytes(file);
    bytes[(int) page.payloadOffset()] = 9; // the version byte
    tamper(file, bytes);
    assertThatThrownBy(() -> read(file))
        .isInstanceOf(ParquetDecodingException.class)
        .hasRootCauseInstanceOf(PcodecException.class)
        .rootCause().hasMessage("pcodec payload: unsupported version 9");
  }

  /**
   * Every sixteenth byte of every payload changed in turn gives the rows or an error, never a
   * crash: the payload checks refuse some, others decode to bytes Parquet's decoders refuse.
   */
  @Test
  void damagedPayloadsNeverCrash() throws IOException {
    java.nio.file.Path file = write(rows(300), WriterVersion.PARQUET_2_0, false);
    byte[] clean = Files.readAllBytes(file);
    List<String> want = read(file);
    int errors = 0;
    int payloadErrors = 0;
    for (PcodecPages.Page page : PcodecPages.pages(file)) {
      for (int i = 0; i < page.payloadLength(); i += Math.max(1, page.payloadLength() / 16)) {
        byte[] bytes = clean.clone();
        bytes[(int) page.payloadOffset() + i] ^= (byte) 0x5a;
        tamper(file, bytes);
        try {
          read(file);
        } catch (IOException | RuntimeException e) {
          errors++;
          payloadErrors += hasPcodecCause(e) ? 1 : 0;
        }
      }
    }
    tamper(file, clean);
    assertThat(read(file)).isEqualTo(want);
    assertThat(errors).isPositive();
    assertThat(payloadErrors).isPositive();
  }

  static boolean hasPcodecCause(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof PcodecException) {
        return true;
      }
    }
    return false;
  }
}
