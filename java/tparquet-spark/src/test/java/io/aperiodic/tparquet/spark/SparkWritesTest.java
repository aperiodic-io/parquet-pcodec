package io.aperiodic.tparquet.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.aperiodic.tparquet.Inspect;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The tparquet data source writes PCODEC files that Spark (and its parquet source) reads back. */
class SparkWritesTest {
  @TempDir Path dir;

  static Dataset<Row> trades(long n) {
    return Spark.session().range(n).selectExpr(
        "id",
        "timestamp_millis(1735689600000 + id * 37) as ts",
        "cast(id % 7 as int) as venue",
        // double literals: 100.0 alone would be a DECIMAL in Spark SQL
        "case when id % 11 = 0 then null else cast(1000 + (id * 7919 % 977) / 100.0D as double) end as price",
        "cast((id % 300) / 4.0 as float) as qty",
        "case when id % 13 = 0 then null else concat('S', cast(id % 5 as string)) end as sym",
        "id % 2 = 0 as buy",
        "array(id, id + 1, case when id % 3 = 0 then null else id + 2 end) as levels",
        "named_struct('bid', 100.0D + id / 8.0D, 'ask', 100.25D + id / 8.0D) as quote",
        "map('a', cast(id as int)) as tags",
        "cast(id * 3 as decimal(18, 4)) as amount",
        "date_add(date'2025-01-01', cast(id % 50 as int)) as day");
  }

  static List<Path> parquetFiles(Path out) throws IOException {
    try (Stream<Path> s = Files.walk(out)) {
      return s.filter(p -> p.getFileName().toString().endsWith(".parquet")).sorted().toList();
    }
  }

  static ParquetMetadata footer(Path file) throws IOException {
    try (ParquetFileReader r = ParquetFileReader.open(new LocalInputFile(file))) {
      return r.getFooter();
    }
  }

  @Test
  void writesPcodecFilesThatReadBack() throws IOException {
    Dataset<Row> df = trades(20_000);
    Path out = dir.resolve("t");
    df.write().format("tparquet").save(out.toString());
    List<Path> files = parquetFiles(out);
    assertThat(files).isNotEmpty().allMatch(p -> p.getFileName().toString().endsWith(".pcodec.parquet"));
    for (Path f : files) {
      ParquetMetadata footer = footer(f);
      for (BlockMetaData b : footer.getBlocks()) {
        for (ColumnChunkMetaData c : b.getColumns()) {
          assertThat(c.getCodec()).isSameAs(CompressionCodecName.PCODEC);
        }
      }
      assertThat(footer.getFileMetaData().getKeyValueMetaData())
          .containsEntry("parquet.pcodec.version", "1").containsKey("org.apache.spark.version");
    }
    List<String> want = Spark.rows(df.orderBy("id"));
    assertThat(Spark.rows(Spark.session().read().format("tparquet").load(out.toString()).orderBy("id"))).isEqualTo(want);
    assertThat(Spark.rows(Spark.session().read().parquet(out.toString()).orderBy("id"))).isEqualTo(want);
  }

  /** Spark's nullable numeric columns are PCODEC's own modes: V2 pages, PLAIN values (spec section 3). */
  @Test
  void numericColumnsAreEncodedForTheirType() throws IOException {
    Path out = dir.resolve("t");
    trades(50_000).coalesce(1).write().format("tparquet").save(out.toString());
    Map<String, List<String>> modes = Inspect.modes(parquetFiles(out).get(0));
    assertThat(modes.get("id")).as(modes.toString()).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("ts")).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("venue")).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("day")).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("amount")).containsOnly("DATA_PAGE_V2:PCO");
    assertThat(modes.get("price")).isSubsetOf("DATA_PAGE_V2:DECIMAL", "DATA_PAGE_V2:PCO").contains("DATA_PAGE_V2:DECIMAL");
    assertThat(modes.get("quote.bid")).isSubsetOf("DATA_PAGE_V2:DECIMAL", "DATA_PAGE_V2:PCO");
    assertThat(modes.get("levels.list.element")).containsOnly("DATA_PAGE_V2:PCO");
  }

  @Test
  void timestampsAreInt64UnlessAskedOtherwise() throws IOException {
    Path micros = dir.resolve("micros");
    trades(1000).coalesce(1).write().format("tparquet").save(micros.toString());
    Path int96 = dir.resolve("int96");
    trades(1000).coalesce(1).write().format("tparquet").option("outputTimestampType", "INT96").save(int96.toString());
    var ts = footer(parquetFiles(micros).get(0)).getFileMetaData().getSchema().getType("ts").asPrimitiveType();
    assertThat(ts.getPrimitiveTypeName().name()).isEqualTo("INT64");
    assertThat(ts.getLogicalTypeAnnotation()).hasToString("TIMESTAMP(MICROS,true)");
    assertThat(footer(parquetFiles(int96).get(0)).getFileMetaData().getSchema().getType("ts").asPrimitiveType()
        .getPrimitiveTypeName().name()).isEqualTo("INT96");
    assertThat(Spark.rows(Spark.session().read().parquet(micros.toString()).orderBy("id")))
        .isEqualTo(Spark.rows(Spark.session().read().parquet(int96.toString()).orderBy("id")));
  }

  @Test
  void theLevelOptionReachesTheEncoder() throws IOException {
    Dataset<Row> df = trades(50_000).coalesce(1);
    df.write().format("tparquet").option("level", "0").save(dir.resolve("l0").toString());
    df.write().format("tparquet").option("level", "12").save(dir.resolve("l12").toString());
    assertThat(Files.size(parquetFiles(dir.resolve("l0")).get(0))).isGreaterThan(Files.size(parquetFiles(dir.resolve("l12")).get(0)));
  }

  @Test
  void badOptionsAreRefused() {
    Dataset<Row> df = trades(10);
    assertThatThrownBy(() -> df.write().format("tparquet").option("level", "13").save(dir.resolve("a").toString()))
        .hasStackTraceContaining("level 13 outside 0..12");
    assertThatThrownBy(() -> df.write().format("tparquet").option("compression", "zstd").save(dir.resolve("b").toString()))
        .hasStackTraceContaining("tparquet writes PCODEC: compression=zstd is not supported");
    df.write().format("tparquet").option("compression", "pcodec").save(dir.resolve("c").toString());
  }

  @Test
  void partitionedAppendsAndSql() throws IOException {
    SparkSession s = Spark.session();
    Path out = dir.resolve("p");
    trades(3000).write().format("tparquet").partitionBy("venue").save(out.toString());
    trades(3000).selectExpr("id + 3000 as id", "ts", "venue", "price", "qty", "sym", "buy", "levels", "quote", "tags", "amount", "day")
        .write().format("tparquet").mode(SaveMode.Append).partitionBy("venue").save(out.toString());
    assertThat(parquetFiles(out)).allMatch(p -> p.getParent().getFileName().toString().startsWith("venue="));
    s.sql("CREATE TABLE trades_t USING tparquet LOCATION '" + out + "'");
    try {
      s.sql("ALTER TABLE trades_t RECOVER PARTITIONS");
      assertThat(s.sql("SELECT count(*), count(distinct venue), sum(id) FROM trades_t").first().toString())
          .isEqualTo("[6000,7,17997000]");
      assertThat(s.sql("SELECT venue, count(*) FROM trades_t WHERE venue = 3 GROUP BY venue").first().toString())
          .isEqualTo("[3,858]");
      s.sql("CREATE TABLE trades_copy USING tparquet AS SELECT * FROM trades_t WHERE price > 1005");
      long n = s.sql("SELECT count(*) FROM trades_t WHERE price > 1005").first().getLong(0);
      assertThat(s.sql("SELECT count(*) FROM trades_copy").first().getLong(0)).isEqualTo(n).isPositive();
      s.sql("INSERT INTO trades_copy SELECT * FROM trades_t WHERE id < 10");
      assertThat(s.sql("SELECT count(*) FROM trades_copy").first().getLong(0)).isEqualTo(n + 10);
    } finally {
      s.sql("DROP TABLE IF EXISTS trades_t");
      s.sql("DROP TABLE IF EXISTS trades_copy");
    }
  }

  /** Row-group statistics of PCODEC files still prune: a selective filter reads fewer rows. */
  @Test
  void filtersPruneRowGroups() throws IOException {
    SparkSession s = Spark.session();
    Path out = dir.resolve("pruned");
    trades(200_000).coalesce(1).write().format("tparquet").option("rowGroupSize", String.valueOf(64 * 1024)).save(out.toString());
    assertThat(footer(parquetFiles(out).get(0)).getBlocks()).hasSizeGreaterThan(4);
    Dataset<Row> q = s.read().parquet(out.toString()).where("id >= 190000");
    assertThat(q.count()).isEqualTo(10_000);
    // the scan read far fewer rows than the file has
    Dataset<Row> counted = s.read().parquet(out.toString()).where("id >= 190000");
    counted.collect();
    long scanned = scannedRows(counted);
    assertThat(scanned).isLessThan(100_000).isGreaterThanOrEqualTo(10_000);
  }

  static long scannedRows(Dataset<Row> df) {
    var plan = df.queryExecution().executedPlan();
    List<Long> rows = new ArrayList<>();
    plan.foreach(node -> {
      if (node.nodeName().contains("Scan")) {
        var m = node.metrics().get("numOutputRows");
        if (m.isDefined()) {
          rows.add((Long) m.get().value());
        }
      }
      return null;
    });
    return rows.stream().mapToLong(Long::longValue).sum();
  }

  /** A corrupt PCODEC payload fails the task with the codec's message, never a crash. */
  @Test
  void aCorruptFileIsAnError() throws IOException {
    Path out = dir.resolve("c");
    trades(5000).coalesce(1).write().format("tparquet").save(out.toString());
    Path f = parquetFiles(out).get(0);
    byte[] bytes = Files.readAllBytes(f);
    // the first page payload of the first column: after its page header, the version byte
    long start = footer(f).getBlocks().get(0).getColumns().get(0).getStartingPos();
    int at = (int) start;
    while (bytes[at] != 1 || bytes[at + 1] < 0 || bytes[at + 1] > 3) {
      at++;
    }
    bytes[at] = 9;
    Files.write(f, bytes);
    Files.deleteIfExists(f.resolveSibling("." + f.getFileName() + ".crc"));
    assertThatThrownBy(() -> Spark.session().read().parquet(out.toString()).collect())
        .hasStackTraceContaining("pcodec payload: unsupported version 9");
  }
}
