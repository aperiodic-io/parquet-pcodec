package io.aperiodic.tparquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/** The conformance vectors of {@code testdata/} (system property {@code tparquet.testdata}). */
final class Vectors_ {
  private Vectors_() {}

  static Path dir() {
    String d = System.getProperty("tparquet.testdata");
    return Path.of(d == null ? "../../testdata" : d);
  }

  static Stream<String> names() {
    try (Stream<Path> files = Files.list(dir().resolve("files"))) {
      List<String> names = files.map(p -> p.getFileName().toString())
          .filter(n -> n.endsWith(".parquet")).map(n -> n.substring(0, n.length() - 8)).sorted().toList();
      return names.stream();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Path file(String name) {
    return dir().resolve("files").resolve(name + ".parquet");
  }

  static List<String> expected(String name) throws IOException {
    return Files.readAllLines(dir().resolve("expected").resolve(name + ".jsonl"));
  }
}
