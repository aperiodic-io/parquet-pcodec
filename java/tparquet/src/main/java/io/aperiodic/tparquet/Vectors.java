package io.aperiodic.tparquet;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.LocalInputFile;

/**
 * The conformance-vector CLI of acceptance/cross/run.sh (the contract of acceptance/cross/vectors.py):
 *
 * <pre>
 *   Vectors check DIR          read DIR/files/*.parquet, compare with DIR/expected/&lt;name&gt;.jsonl
 *   Vectors write OUT SRC...   rewrite every SRC vector with TparquetWriter into OUT
 * </pre>
 *
 * {@code check} prints {@code <n> vectors match}, or names the first differing line and exits 1.
 */
public final class Vectors {
  private Vectors() {}

  public static void main(String[] args) {
    // parquet-java logs every row group it reads at INFO
    if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null) {
      System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    }
    System.exit(run(args, System.out, System.err));
  }

  static final class Mismatch extends Exception {
    private static final long serialVersionUID = 1L;

    Mismatch(String message) {
      super(message);
    }
  }

  static int run(String[] args, PrintStream out, PrintStream err) {
    try {
      if (args.length == 2 && args[0].equals("check")) {
        out.println(check(Path.of(args[1])) + " vectors match");
        return 0;
      }
      if (args.length >= 3 && args[0].equals("write")) {
        Path dst = Path.of(args[1]);
        Set<String> written = new HashSet<>();
        for (int i = 2; i < args.length; i++) {
          write(dst, Path.of(args[i]), written);
        }
        return 0;
      }
      err.println("usage: Vectors check DIR | Vectors write OUT SRC...");
      return 2;
    } catch (Mismatch e) {
      err.println(e.getMessage());
      return 1;
    } catch (IOException e) {
      err.println(e);
      return 1;
    }
  }

  /** (name, file, expected) of every vector of {@code dir}. */
  static List<Path> files(Path dir) throws IOException, Mismatch {
    Path files = dir.resolve("files");
    List<Path> out;
    if (!Files.isDirectory(files)) {
      out = List.of();
    } else {
      try (Stream<Path> s = Files.list(files)) {
        out = s.filter(p -> p.getFileName().toString().endsWith(".parquet")).sorted().collect(Collectors.toList());
      }
    }
    if (out.isEmpty()) {
      throw new Mismatch("no files/*.parquet in " + dir);
    }
    for (Path f : out) {
      String name = name(f);
      if (!Files.isRegularFile(dir.resolve("expected").resolve(name + ".jsonl"))) {
        throw new Mismatch(name + ": no expected/" + name + ".jsonl");
      }
    }
    return out;
  }

  static String name(Path file) {
    String n = file.getFileName().toString();
    return n.substring(0, n.length() - ".parquet".length());
  }

  static int check(Path dir) throws IOException, Mismatch {
    int n = 0;
    for (Path f : files(dir)) {
      String name = name(f);
      List<String> got = Canonical.lines(f);
      List<String> want = Files.readAllLines(dir.resolve("expected").resolve(name + ".jsonl"));
      for (int i = 0; i < Math.max(got.size(), want.size()); i++) {
        String a = i < got.size() ? got.get(i) : null;
        String b = i < want.size() ? want.get(i) : null;
        if (a == null || !a.equals(b)) {
          throw new Mismatch(name + ": line " + (i + 1) + " differs from expected/" + name + ".jsonl");
        }
      }
      n++;
    }
    return n;
  }

  static void write(Path dst, Path src, Set<String> written) throws IOException, Mismatch {
    Files.createDirectories(dst.resolve("files"));
    Files.createDirectories(dst.resolve("expected"));
    for (Path f : files(src)) {
      String name = name(f);
      if (!written.add(name)) {
        throw new Mismatch(name + ": written twice (two sources have a vector of that name)");
      }
      FileMetaData meta;
      try (ParquetFileReader r = ParquetFileReader.open(new LocalInputFile(f))) {
        meta = r.getFooter().getFileMetaData();
      }
      // the source's key-value metadata (e.g. ARROW:schema), but the codec key, which the writer adds
      Map<String, String> kv = new HashMap<>(meta.getKeyValueMetaData());
      kv.remove(Pcodec.KV_KEY);
      Path out = dst.resolve("files").resolve(name + ".parquet");
      try (TparquetReader r = TparquetReader.open(f);
          ParquetWriter<Group> w = TparquetWriter.groups(out, meta.getSchema(), TparquetWriter.Options.defaults()
              .withRowGroupSize(1 << 20).withPageSize(64 << 10).withExtraMetadata(kv))) {
        for (Group g = r.read(); g != null; g = r.read()) {
          w.write(g);
        }
      }
      Files.copy(src.resolve("expected").resolve(name + ".jsonl"), dst.resolve("expected").resolve(name + ".jsonl"),
          java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
