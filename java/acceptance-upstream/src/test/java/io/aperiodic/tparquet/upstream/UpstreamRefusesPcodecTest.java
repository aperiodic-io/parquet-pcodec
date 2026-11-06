package io.aperiodic.tparquet.upstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Unmodified parquet-java refuses every PCODEC file instead of returning wrong data (spec section 2). */
class UpstreamRefusesPcodecTest {
  static Path dir() {
    return Path.of(System.getProperty("tparquet.testdata", "../../testdata")).resolve("files");
  }

  static Stream<String> pcodecVectors() {
    try (Stream<Path> s = Files.list(dir())) {
      List<String> names = s.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".parquet"))
          .map(n -> n.substring(0, n.length() - 8)).filter(n -> !n.equals("standard_control")).sorted().toList();
      return names.stream();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static final class Builder extends ParquetReader.Builder<Group> {
    Builder(Path file) {
      super(new LocalInputFile(file));
    }

    @Override
    protected ReadSupport<Group> getReadSupport() {
      return new GroupReadSupport();
    }
  }

  static long rows(Path file) throws IOException {
    long n = 0;
    try (ParquetReader<Group> r = new Builder(file).build()) {
      while (r.read() != null) {
        n++;
      }
    }
    return n;
  }

  @Test
  void thisIsUpstreamParquetJava() throws Exception {
    String jar = ParquetReader.class.getProtectionDomain().getCodeSource().getLocation().toString();
    assertThat(jar).endsWith("parquet-hadoop-1.17.0.jar");
    assertThat(org.apache.parquet.format.CompressionCodec.findByValue(20547)).isNull();
    assertThat(pcodecVectors().count()).isGreaterThanOrEqualTo(12);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("pcodecVectors")
  void refusesEveryPcodecVector(String name) {
    Path file = dir().resolve(name + ".parquet");
    // the footer's codec is unknown to thrift: the column chunk metadata is refused
    assertThatThrownBy(() -> rows(file)).hasStackTraceContaining("Required field 'codec' was not present!");
  }

  @Test
  void readsTheStandardControl() throws Exception {
    assertThat(rows(dir().resolve("standard_control.parquet"))).isPositive();
  }
}
