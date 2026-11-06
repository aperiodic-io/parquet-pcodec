package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The CLI as acceptance/cross/run.sh runs it: a fresh JVM, its exit code, no INFO logging. */
class VectorsProcessTest {
  static String[] run(String... args) throws Exception {
    List<String> cmd = new java.util.ArrayList<>(List.of(
        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Dtparquet.jni.library=" + System.getProperty("tparquet.jni.library"),
        "-cp", System.getProperty("java.class.path"), Vectors.class.getName()));
    cmd.addAll(List.of(args));
    Process p = new ProcessBuilder(cmd).start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    String err = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    return new String[] {String.valueOf(p.waitFor()), out, err};
  }

  @Test
  void checkPrintsTheCountAndExitsZero() throws Exception {
    String[] r = run("check", Vectors_.dir().toString());
    assertThat(r[1]).isEqualTo(Vectors_.names().count() + " vectors match\n");
    assertThat(r[2]).doesNotContain("INFO");
    assertThat(r[0]).isEqualTo("0");
  }

  @Test
  void usageExitsTwo() throws Exception {
    assertThat(run()[0]).isEqualTo("2");
  }
}
