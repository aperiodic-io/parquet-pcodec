package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Every route {@link NativeLibrary} loads the library by, each in a fresh JVM. */
class NativeLibraryProcessTest {
  static final Path LIBRARY = Path.of(System.getProperty(NativeLibrary.PROPERTY));

  record Result(int exit, String output) {}

  static Result run(String classpath, String... jvmArgs) throws Exception {
    List<String> cmd = new ArrayList<>();
    cmd.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    cmd.addAll(List.of(jvmArgs));
    cmd.add("-cp");
    cmd.add(classpath);
    cmd.add(LoadProbe.class.getName());
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new Result(p.waitFor(), out);
  }

  /** The main classes, copied without a bundled library (a packaged build puts one there). */
  static Path classes;

  @BeforeAll
  static void classesWithoutTheLibrary(@TempDir Path dir) throws Exception {
    Path main = Path.of(Pcodec.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    classes = dir.resolve("classes");
    try (Stream<Path> files = Files.walk(main)) {
      for (Path f : (Iterable<Path>) files::iterator) {
        Path rel = main.relativize(f);
        if (rel.startsWith(Path.of("io", "aperiodic", "tparquet", "native"))) {
          continue;
        }
        Path to = classes.resolve(rel.toString());
        if (Files.isDirectory(f)) {
          Files.createDirectories(to);
        } else {
          Files.copy(f, to);
        }
      }
    }
  }

  /** This JVM's class path with {@link #classes} for the main classes, then {@code extra}. */
  static String classpath(Path... extra) throws Exception {
    Path main = Path.of(Pcodec.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    String cp = Stream.of(System.getProperty("java.class.path").split(File.pathSeparator))
        .map(e -> Path.of(e).equals(main) ? classes.toString() : e)
        .collect(Collectors.joining(File.pathSeparator));
    for (Path p : extra) {
      cp += File.pathSeparator + p;
    }
    return cp;
  }

  @Test
  void fromTheSystemProperty() throws Exception {
    Result r = run(classpath(), "-D" + NativeLibrary.PROPERTY + "=" + LIBRARY);
    assertThat(r.output()).contains("round trip ok");
    assertThat(r.exit()).isZero();
  }

  @Test
  void fromTheJarResource(@TempDir Path dir) throws Exception {
    String resource = NativeLibrary.resourcePath(System.getProperty("os.name"), System.getProperty("os.arch"));
    Path lib = dir.resolve(resource.substring(1));
    Files.createDirectories(lib.getParent());
    Files.copy(LIBRARY, lib);
    Result r = run(classpath(dir));
    assertThat(r.output()).contains("round trip ok");
    assertThat(r.exit()).isZero();
  }

  @Test
  void fromTheLibraryPath() throws Exception {
    Result r = run(classpath(), "-Djava.library.path=" + LIBRARY.getParent());
    assertThat(r.output()).contains("round trip ok");
    assertThat(r.exit()).isZero();
  }

  @Test
  void withoutAnyRouteLoadingFails() throws Exception {
    Result r = run(classpath(), "-Djava.library.path=/nonexistent");
    assertThat(r.output()).contains("UnsatisfiedLinkError").contains("no tparquet_jni in java.library.path");
    assertThat(r.exit()).isNotZero();
  }

  @Test
  void aPropertyNamingNoFileFails() throws Exception {
    Result r = run(classpath(), "-D" + NativeLibrary.PROPERTY + "=/nonexistent/lib.so", "-Djava.library.path=" + LIBRARY.getParent());
    assertThat(r.output()).contains("tparquet.jni.library=/nonexistent/lib.so: no such file");
    assertThat(r.exit()).isNotZero();
  }
}
