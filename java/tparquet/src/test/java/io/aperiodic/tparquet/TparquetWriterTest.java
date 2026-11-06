package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** TparquetWriter follows the writer recommendations of spec section 3. */
class TparquetWriterTest {
  static final MessageType SCHEMA = MessageTypeParser.parseMessageType(String.join("\n",
      "message trades {",
      "  required int64 ts (TIMESTAMP(MILLIS,true));",
      "  optional double price;",
      "  optional float qty;",
      "  optional int32 venue;",
      "  required binary sym (STRING);",
      "  optional group levels (LIST) { repeated group list { optional int64 element; } }",
      "}"));

  @TempDir Path dir;

  static List<Group> rows(int n) {
    SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);
    List<Group> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Group g = f.newGroup().append("ts", 1_735_689_600_000L + 13L * i).append("sym", "S" + i % 5);
      if (i % 9 != 0) {
        g.append("price", (100_000 + (i * 7919) % 977) / 100.0);
      }
      if (i % 4 != 0) {
        g.append("qty", (i % 300) / 4f);
      }
      if (i % 3 != 0) {
        g.append("venue", i % 7);
      }
      Group l = g.addGroup("levels");
      for (int j = 0; j < i % 4; j++) {
        l.addGroup("list").append("element", (long) (i + j));
      }
      out.add(g);
    }
    return out;
  }

  Path write(List<Group> rows, TparquetWriter.Options options) throws IOException {
    Path file = dir.resolve("t-" + System.nanoTime() + ".parquet");
    try (var w = TparquetWriter.groups(file, SCHEMA, options)) {
      for (Group g : rows) {
        w.write(g);
      }
    }
    return file;
  }

  static ParquetMetadata footer(Path file) throws IOException {
    try (ParquetFileReader r = ParquetFileReader.open(new LocalInputFile(file))) {
      return r.getFooter();
    }
  }

  @Test
  void numericColumnsArePcodecPayloadsOfTheirType() throws IOException {
    Path file = write(rows(30_000), TparquetWriter.Options.defaults());
    Map<String, List<String>> modes = Inspect.modes(file);
    assertThat(modes.get("ts")).as(modes.toString()).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("venue")).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("levels.list.element")).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("price")).isSubsetOf("DATA_PAGE_V2:DECIMAL", "DATA_PAGE_V2:PCO").contains("DATA_PAGE_V2:DECIMAL");
    assertThat(modes.get("qty")).isSubsetOf("DATA_PAGE_V2:DECIMAL", "DATA_PAGE_V2:PCO");
    // strings keep their dictionary
    assertThat(modes.get("sym")).anyMatch(m -> m.startsWith("DICTIONARY_PAGE:"));
  }

  @Test
  void numericColumnsArePlainWithoutADictionaryAndTheFileSaysPcodec() throws IOException {
    ParquetMetadata footer = footer(write(rows(5000), TparquetWriter.Options.defaults()));
    for (BlockMetaData b : footer.getBlocks()) {
      for (ColumnChunkMetaData c : b.getColumns()) {
        assertThat(c.getCodec()).isSameAs(CompressionCodecName.PCODEC);
        if (!c.getPath().toDotString().equals("sym")) {
          assertThat(c.getEncodings()).as(c.getPath().toDotString()).doesNotContain(Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY)
              .contains(Encoding.PLAIN);
        }
      }
    }
    assertThat(footer.getFileMetaData().getKeyValueMetaData()).containsEntry("parquet.pcodec.version", "1");
  }

  @Test
  void rowsRoundTrip() throws IOException {
    List<Group> rows = rows(10_000);
    Path file = write(rows, TparquetWriter.Options.defaults());
    List<String> want = rows.stream().map(Canonical::line).toList();
    assertThat(Canonical.lines(file)).isEqualTo(want);
  }

  /** Unlike parquet-java's own PLAIN writer, NaN payloads are written bit for bit. */
  @Test
  void nanPayloadsAreKept() throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required double d; optional float f; }");
    Path file = dir.resolve("nan.parquet");
    long[] d = {0x7ff8000000000001L, 0xfff800000000beefL, 0x7ff0000000000001L, 0x8000000000000000L};
    int[] f = {0x7fc00001, 0xffc0beef, 0x7f800001, 0x80000000};
    try (var w = TparquetWriter.groups(file, schema, TparquetWriter.Options.defaults())) {
      SimpleGroupFactory g = new SimpleGroupFactory(schema);
      for (int i = 0; i < 400; i++) {
        w.write(g.newGroup().append("d", Double.longBitsToDouble(d[i % 4])).append("f", Float.intBitsToFloat(f[i % 4])));
      }
    }
    List<String> lines = Canonical.lines(file);
    assertThat(lines).hasSize(400);
    for (int i = 0; i < 4; i++) {
      assertThat(lines.get(i)).isEqualTo(String.format("{\"d\":\"0x%016x\",\"f\":\"0x%08x\"}", d[i], f[i]));
    }
  }

  @Test
  void theOptionsReachTheWriter() throws IOException {
    TparquetWriter.Options o = TparquetWriter.Options.defaults().withRowGroupSize(32 * 1024).withPageSize(8 * 1024)
        .withExtraMetadata(Map.of("origin", "test"));
    Path file = write(rows(30_000), o.withLevel(0));
    ParquetMetadata footer = footer(file);
    assertThat(footer.getBlocks()).hasSizeGreaterThan(1);
    assertThat(footer.getFileMetaData().getKeyValueMetaData()).containsEntry("origin", "test");
    // the same options but the level: level 0 is larger
    Path l12 = write(rows(30_000), o.withLevel(12));
    assertThat(java.nio.file.Files.size(file)).isGreaterThan(java.nio.file.Files.size(l12));
    // pages are cut at the page size: 30000 timestamps are 240 KB, so dozens of 8 KB pages
    Path paged = write(rows(30_000), TparquetWriter.Options.defaults().withPageSize(8 * 1024));
    assertThat(footer(paged).getBlocks()).hasSize(1);
    assertThat(Inspect.pageList(paged).get("ts")).hasSizeGreaterThan(10);
    // with 1 MiB pages, only parquet-java's 20000-row page limit cuts them
    assertThat(Inspect.pageList(write(rows(30_000), TparquetWriter.Options.defaults())).get("ts")).hasSize(2);
  }

  @Test
  void theDecimalOptionReachesTheEncoder() throws IOException {
    Path off = write(rows(30_000), TparquetWriter.Options.defaults().withDecimal(false));
    assertThat(Inspect.modes(off).get("price")).containsOnly("DATA_PAGE_V2:PCO");
    Path on = write(rows(30_000), TparquetWriter.Options.defaults().withDecimal(true));
    assertThat(Inspect.modes(on).get("price")).contains("DATA_PAGE_V2:DECIMAL");
  }

  @Test
  void levelsOutsideZeroToTwelveAreRefused() {
    for (int level : new int[] {-1, 13}) {
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> TparquetWriter.Options.defaults().withLevel(level))
          .isInstanceOf(IllegalArgumentException.class).hasMessage("level " + level + " outside 0..12");
    }
    TparquetWriter.Options.defaults().withLevel(0).withLevel(12);
  }

  @Test
  void anotherCodecWritesAStandardFile() throws IOException {
    Path file = write(rows(1000), TparquetWriter.Options.defaults().withCodec(CompressionCodecName.ZSTD));
    ParquetMetadata footer = footer(file);
    assertThat(footer.getBlocks().get(0).getColumns()).allMatch(c -> c.getCodec() == CompressionCodecName.ZSTD);
    assertThat(footer.getFileMetaData().getKeyValueMetaData()).doesNotContainKey("parquet.pcodec.version");
    assertThat(Canonical.lines(file)).hasSize(1000);
  }

  @Test
  void anEmptyFile() throws IOException {
    Path file = write(List.of(), TparquetWriter.Options.defaults());
    assertThat(Canonical.lines(file)).isEmpty();
  }
}
