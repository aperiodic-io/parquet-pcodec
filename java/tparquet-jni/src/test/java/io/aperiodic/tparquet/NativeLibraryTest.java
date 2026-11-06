package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class NativeLibraryTest {
  static final String LIBRARY = System.getProperty(NativeLibrary.PROPERTY);

  @Test
  void resourcePathPerPlatform() {
    assertThat(NativeLibrary.resourcePath("Linux", "amd64"))
        .isEqualTo("/io/aperiodic/tparquet/native/linux-x86_64/libtparquet_jni.so");
    assertThat(NativeLibrary.resourcePath("Linux", "aarch64"))
        .isEqualTo("/io/aperiodic/tparquet/native/linux-aarch64/libtparquet_jni.so");
    assertThat(NativeLibrary.resourcePath("Mac OS X", "aarch64"))
        .isEqualTo("/io/aperiodic/tparquet/native/darwin-aarch64/libtparquet_jni.dylib");
    assertThat(NativeLibrary.resourcePath("Darwin", "x86_64"))
        .isEqualTo("/io/aperiodic/tparquet/native/darwin-x86_64/libtparquet_jni.dylib");
    assertThat(NativeLibrary.resourcePath("Windows 11", "amd64"))
        .isEqualTo("/io/aperiodic/tparquet/native/windows-x86_64/tparquet_jni.dll");
    assertThat(NativeLibrary.resourcePath("Mac OS X", "arm64"))
        .isEqualTo("/io/aperiodic/tparquet/native/darwin-aarch64/libtparquet_jni.dylib");
    assertThat(NativeLibrary.resourcePath("FreeBSD", "ppc64le"))
        .isEqualTo("/io/aperiodic/tparquet/native/freebsd-ppc64le/libtparquet_jni.so");
  }

  @Test
  void aMissingResourceIsNotExtracted() {
    assertThat(NativeLibrary.extract("/x/libnone.so", r -> null)).isNull();
  }

  @Test
  void aResourceIsExtractedToATemporaryFile() throws Exception {
    Path p = NativeLibrary.extract("/a/b/extract-me.txt",
        r -> new ByteArrayInputStream((r + "\n").getBytes(StandardCharsets.UTF_8)));
    assertThat(p.getFileName()).hasToString("extract-me.txt");
    assertThat(Files.readString(p)).isEqualTo("/a/b/extract-me.txt\n");
  }

  @Test
  void aResourceThatCannotBeReadIsAnError() {
    InputStream broken = new InputStream() {
      @Override
      public int read() throws IOException {
        throw new IOException("broken");
      }
    };
    assertThatThrownBy(() -> NativeLibrary.extract("/x/lib.so", r -> broken))
        .isInstanceOf(UncheckedIOException.class).hasMessage("extracting /x/lib.so");
  }

  @Test
  void aLibraryPropertyNamingNoFileFails() {
    assertThatThrownBy(() -> NativeLibrary.loadFile("/nonexistent/libtparquet_jni.so"))
        .isInstanceOf(UnsatisfiedLinkError.class)
        .hasMessage("tparquet.jni.library=/nonexistent/libtparquet_jni.so: no such file");
    assertThatThrownBy(() -> NativeLibrary.loadFrom("/nonexistent/lib.so", "/r", r -> null))
        .hasMessage("tparquet.jni.library=/nonexistent/lib.so: no such file");
  }

  /** In this JVM the library is loaded already; loading it again by any route is a no-op. */
  @Test
  void loadFromAFileOrAnExtractedResource() {
    assertThat(NativeLibrary.loadFrom(LIBRARY, "/unused", r -> null)).isEqualTo(1);
    assertThat(NativeLibrary.loadFrom(null, "/io/libtparquet_jni.so", r -> open(LIBRARY))).isEqualTo(1);
    assertThat(NativeLibrary.loadFrom("", "/io/libtparquet_jni.so", r -> open(LIBRARY))).isEqualTo(1);
  }

  @Test
  void withoutAFileOrAResourceTheLibraryPathIsSearched() {
    for (String file : new String[] {null, ""}) {
      assertThatThrownBy(() -> NativeLibrary.loadFrom(file, "/none", r -> null))
          .isInstanceOf(UnsatisfiedLinkError.class)
          .hasMessageContaining("no tparquet_jni in java.library.path");
    }
  }

  @Test
  void theAbiVersionIsChecked() {
    assertThat(NativeLibrary.checkAbi(1)).isEqualTo(1);
    assertThatThrownBy(() -> NativeLibrary.checkAbi(2))
        .isInstanceOf(UnsatisfiedLinkError.class).hasMessage("tparquet_jni ABI version 2, expected 1");
    assertThatThrownBy(() -> NativeLibrary.checkAbi(0)).isInstanceOf(UnsatisfiedLinkError.class);
  }

  @Test
  void theLibraryIsLoadedOnce() {
    assertThat(NativeLibrary.load()).isEqualTo(1);
    assertThat(NativeLibrary.load()).isEqualTo(1);
  }

  static InputStream open(String file) {
    try {
      return Files.newInputStream(Path.of(file));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
