package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Every testdata/files vector read through the parquet-java fork gives exactly its expected rows. */
class ConformanceTest {
  static java.util.stream.Stream<String> names() {
    return Vectors_.names();
  }

  @Test
  void allVectorsAreHere() {
    assertThat(names().toList()).hasSizeGreaterThanOrEqualTo(13).contains("standard_control", "float_specials", "mixed");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("names")
  void readsTheExpectedRows(String name) throws Exception {
    List<String> got = Canonical.lines(Vectors_.file(name));
    List<String> want = Vectors_.expected(name);
    for (int i = 0; i < Math.max(got.size(), want.size()); i++) {
      assertThat(i < got.size() ? got.get(i) : null).as("%s line %d", name, i + 1).isEqualTo(i < want.size() ? want.get(i) : null);
    }
  }
}
