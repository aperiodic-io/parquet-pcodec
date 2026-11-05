package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TparquetReaderTest {
  @Test
  void readsRowsThenNull() throws Exception {
    try (TparquetReader r = TparquetReader.open(Vectors_.file("primitives_required"))) {
      int n = 0;
      while (r.read() != null) {
        n++;
      }
      assertThat(n).isEqualTo(Vectors_.expected("primitives_required").size());
      assertThat(r.read()).isNull();
    }
  }
}
