package io.aperiodic.tparquet.spark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** The tests' local SparkSession, and rows as strings that keep float bits. */
final class Spark {
  private static SparkSession session;

  private Spark() {}

  static synchronized SparkSession session() {
    if (session == null) {
      session = SparkSession.builder()
          .master("local[2]")
          .appName("tparquet-spark-tests")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.driver.host", "127.0.0.1")
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate();
    }
    return session;
  }

  static Path testdata() {
    return Path.of(System.getProperty("tparquet.testdata", "../../testdata"));
  }

  static List<String> vectorNames() {
    try (Stream<Path> s = Files.list(testdata().resolve("files"))) {
      return s.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".parquet"))
          .map(n -> n.substring(0, n.length() - 8)).sorted().toList();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Every row of {@code df}, in order, as a string with exact float bits. */
  static List<String> rows(Dataset<Row> df) {
    StructType schema = df.schema();
    List<String> out = new ArrayList<>();
    for (Row r : df.collectAsList()) {
      out.add(struct(r, schema));
    }
    return out;
  }

  static String struct(Row r, StructType t) {
    StringBuilder sb = new StringBuilder("{");
    StructField[] fields = t.fields();
    for (int i = 0; i < fields.length; i++) {
      sb.append(i == 0 ? "" : ",").append(fields[i].name()).append('=').append(value(r.get(i), fields[i].dataType()));
    }
    return sb.append('}').toString();
  }

  static String value(Object v, DataType t) {
    if (v == null) {
      return "null";
    }
    if (t instanceof DoubleType) {
      return String.format("d%016x", Double.doubleToRawLongBits((Double) v));
    }
    if (t instanceof FloatType) {
      return String.format("f%08x", Float.floatToRawIntBits((Float) v));
    }
    if (t instanceof StructType) {
      return struct((Row) v, (StructType) t);
    }
    if (t instanceof ArrayType) {
      StringBuilder sb = new StringBuilder("[");
      List<?> l = scala.jdk.javaapi.CollectionConverters.asJava((scala.collection.Seq<?>) v);
      for (int i = 0; i < l.size(); i++) {
        sb.append(i == 0 ? "" : ",").append(value(l.get(i), ((ArrayType) t).elementType()));
      }
      return sb.append(']').toString();
    }
    if (t instanceof MapType) {
      StringBuilder sb = new StringBuilder("<");
      Map<?, ?> m = scala.jdk.javaapi.CollectionConverters.asJava((scala.collection.Map<?, ?>) v);
      m.forEach((k, x) -> sb.append(value(k, ((MapType) t).keyType())).append("->").append(value(x, ((MapType) t).valueType())).append(';'));
      return sb.append('>').toString();
    }
    if (v instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) v);
    }
    return v.toString();
  }
}
