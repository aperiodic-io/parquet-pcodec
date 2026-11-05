package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The vectors CLI of acceptance/cross/run.sh: `check DIR` and `write OUT SRC...`. */
class VectorsTest {
  @TempDir Path dir;

  record Run(int exit, String out, String err) {}

  static Run run(String... args) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int exit = Vectors.run(args, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
    return new Run(exit, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
  }

  @Test
  void checkTheRepositoryVectors() {
    Run r = run("check", Vectors_.dir().toString());
    assertThat(r.err()).isEmpty();
    assertThat(r.out()).isEqualTo(Vectors_.names().count() + " vectors match\n");
    assertThat(r.exit()).isZero();
  }

  @Test
  void writeThenCheckTheJavaVectors() throws Exception {
    Path out = dir.resolve("java");
    Run w = run("write", out.toString(), Vectors_.dir().toString());
    assertThat(w.err()).isEmpty();
    assertThat(w.exit()).isZero();
    long n = Vectors_.names().count();
    try (var files = Files.list(out.resolve("files"))) {
      assertThat(files.count()).isEqualTo(n);
    }
    Run c = run("check", out.toString());
    assertThat(c.out()).isEqualTo(n + " vectors match\n");
    assertThat(c.exit()).isZero();
    // written by this writer: PCODEC, with the footer key
    assertThat(Inspect.modes(out.resolve("files").resolve("primitives_required.parquet")).get("i64"))
        .containsOnly("DATA_PAGE_V2:PCO");
  }

  @Test
  void writeNamesClashingVectorsAfterTheirSource() throws Exception {
    Path a = dir.resolve("a");
    Files.createDirectories(a.resolve("files"));
    Files.createDirectories(a.resolve("expected"));
    Files.copy(Vectors_.file("mixed"), a.resolve("files/mixed.parquet"));
    Files.copy(Vectors_.dir().resolve("expected/mixed.jsonl"), a.resolve("expected/mixed.jsonl"));
    Run w = run("write", dir.resolve("out").toString(), a.toString(), a.toString());
    assertThat(w.exit()).isEqualTo(1);
    assertThat(w.err()).isEqualTo("mixed: written twice (two sources have a vector of that name)\n");
  }

  @Test
  void aChangedRowIsRefused() throws Exception {
    Path bad = dir.resolve("bad");
    Files.createDirectories(bad.resolve("files"));
    Files.createDirectories(bad.resolve("expected"));
    Files.copy(Vectors_.file("temporal"), bad.resolve("files/temporal.parquet"));
    List<String> rows = Vectors_.expected("temporal");
    rows.set(1, rows.get(1).replaceFirst("1", "2"));
    Files.write(bad.resolve("expected/temporal.jsonl"), rows);
    Run r = run("check", bad.toString());
    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.err()).isEqualTo("temporal: line 2 differs from expected/temporal.jsonl\n");
  }

  @Test
  void aMissingOrExtraLineIsRefused() throws Exception {
    Path bad = dir.resolve("bad");
    Files.createDirectories(bad.resolve("files"));
    Files.createDirectories(bad.resolve("expected"));
    Files.copy(Vectors_.file("temporal"), bad.resolve("files/temporal.parquet"));
    List<String> rows = Vectors_.expected("temporal");
    Files.write(bad.resolve("expected/temporal.jsonl"), rows.subList(0, rows.size() - 1));
    assertThat(run("check", bad.toString()).err()).isEqualTo("temporal: line " + rows.size() + " differs from expected/temporal.jsonl\n");
  }

  @Test
  void aVectorWithoutExpectedRowsIsRefused() throws Exception {
    Path bad = dir.resolve("bad");
    Files.createDirectories(bad.resolve("files"));
    Files.copy(Vectors_.file("temporal"), bad.resolve("files/temporal.parquet"));
    Run r = run("check", bad.toString());
    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.err()).isEqualTo("temporal: no expected/temporal.jsonl\n");
  }

  @Test
  void aDirectoryWithoutVectorsIsRefused() {
    Run r = run("check", dir.toString());
    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.err()).isEqualTo("no files/*.parquet in " + dir + "\n");
  }

  @Test
  void usage() {
    for (String[] args : new String[][] {{}, {"check"}, {"write", "out"}, {"frobnicate", "x"}}) {
      Run r = run(args);
      assertThat(r.exit()).isEqualTo(2);
      assertThat(r.err()).startsWith("usage: ");
    }
  }
}
