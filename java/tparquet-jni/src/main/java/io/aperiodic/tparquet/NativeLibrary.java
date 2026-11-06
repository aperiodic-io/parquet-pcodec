package io.aperiodic.tparquet;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.function.Function;

/**
 * Loads the JNI library ({@code rust/jni}), once per class loader, from the first of:
 *
 * <ol>
 *   <li>the file named by the system property {@value #PROPERTY};
 *   <li>the copy in this jar, under {@link #resourcePath}, extracted to a temporary file;
 *   <li>{@code java.library.path} ({@code System.loadLibrary("tparquet_jni")}).
 * </ol>
 */
final class NativeLibrary {
  static final String PROPERTY = "tparquet.jni.library";
  static final String NAME = "tparquet_jni";
  /** The ABI version this class expects of the library ({@code rust/jni/src/lib.rs}). */
  static final int ABI_VERSION = 1;

  private NativeLibrary() {}

  /**
   * Loads the library unless this class loader already has (class initialization runs once).
   *
   * @return the library's ABI version, {@link #ABI_VERSION}
   */
  static int load() {
    return Once.ABI;
  }

  private static final class Once {
    static final int ABI = loadFrom(
        System.getProperty(PROPERTY),
        resourcePath(System.getProperty("os.name"), System.getProperty("os.arch")),
        NativeLibrary.class::getResourceAsStream);
  }

  /**
   * Loads {@code file} if set, else {@code resource} if {@code opener} has it, else {@value #NAME}.
   *
   * @return the library's ABI version, checked
   */
  static int loadFrom(String file, String resource, Function<String, InputStream> opener) {
    if (file != null && !file.isEmpty()) {
      loadFile(file);
    } else {
      Path extracted = extract(resource, opener);
      if (extracted != null) {
        System.load(extracted.toString());
      } else {
        System.loadLibrary(NAME);
      }
    }
    return checkAbi(Pcodec.nativeAbiVersion());
  }

  /** Loads the library named by {@value #PROPERTY}. */
  static void loadFile(String file) {
    if (!Files.isRegularFile(Path.of(file))) {
      throw new UnsatisfiedLinkError(PROPERTY + "=" + file + ": no such file");
    }
    System.load(file);
  }

  static int checkAbi(int abi) {
    if (abi != ABI_VERSION) {
      throw new UnsatisfiedLinkError("tparquet_jni ABI version " + abi + ", expected " + ABI_VERSION);
    }
    return abi;
  }

  /** Where the jar holds the library for a platform, e.g. {@code /io/aperiodic/tparquet/native/linux-x86_64/libtparquet_jni.so}. */
  static String resourcePath(String osName, String osArch) {
    String os = osName.toLowerCase(Locale.ROOT);
    String file;
    if (os.startsWith("mac") || os.startsWith("darwin")) {
      os = "darwin";
      file = "lib" + NAME + ".dylib";
    } else if (os.startsWith("windows")) {
      os = "windows";
      file = NAME + ".dll";
    } else {
      os = os.replaceAll("[^a-z0-9]", "");
      file = "lib" + NAME + ".so";
    }
    String arch = osArch.toLowerCase(Locale.ROOT);
    switch (arch) {
      case "amd64":
      case "x86_64":
        arch = "x86_64";
        break;
      case "arm64":
      case "aarch64":
        arch = "aarch64";
        break;
      default:
        arch = arch.replaceAll("[^a-z0-9_]", "");
    }
    return "/io/aperiodic/tparquet/native/" + os + "-" + arch + "/" + file;
  }

  /** Copies the resource to a temporary file; {@code null} when {@code opener} does not have it. */
  static Path extract(String resource, Function<String, InputStream> opener) {
    try (InputStream in = opener.apply(resource)) {
      if (in == null) {
        return null;
      }
      Path dir = Files.createTempDirectory("tparquet-jni");
      Path lib = dir.resolve(resource.substring(resource.lastIndexOf('/') + 1));
      Files.copy(in, lib, StandardCopyOption.REPLACE_EXISTING);
      lib.toFile().deleteOnExit();
      dir.toFile().deleteOnExit();
      return lib;
    } catch (IOException e) {
      throw new UncheckedIOException("extracting " + resource, e);
    }
  }
}
