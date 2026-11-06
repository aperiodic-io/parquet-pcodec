package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/** The codec is called from many threads at once; failures on one never reach another. */
class ThreadsTest {
  @Test
  void concurrentRoundTripsAndFailures() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<?>> done = new ArrayList<>();
      for (int t = 0; t < 32; t++) {
        int seed = t;
        done.add(pool.submit(() -> {
          for (int i = 0; i < 40; i++) {
            PhysicalType type = PhysicalType.values()[(seed + i) % 4];
            byte[] src = PcodecTest.plain(type, 500 + seed * 7 + i);
            byte[] p = Pcodec.compress(src, type);
            if ((seed + i) % 3 == 0) {
              // a failure in between, on this thread
              try {
                Pcodec.decompress(p, src.length + type.width());
                throw new AssertionError("decoded to the wrong size");
              } catch (PcodecException e) {
                assertThat(e.code()).isEqualTo(PcodecException.Code.SIZE);
              }
            }
            assertThat(Pcodec.decompress(p, src.length)).isEqualTo(src);
          }
          return null;
        }));
      }
      for (Future<?> f : done) {
        f.get();
      }
    } finally {
      pool.shutdownNow();
    }
  }
}
