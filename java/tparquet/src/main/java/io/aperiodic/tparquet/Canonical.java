package io.aperiodic.tparquet;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Canonical JSON of rows ({@code testdata/README.md}, "Canonical JSON"): one compact JSON object
 * per row, the way every tparquet implementation writes it, so that rows compare byte for byte.
 */
public final class Canonical {
  private static final long JULIAN_EPOCH_DAY = 2_440_588L;
  private static final long NANOS_PER_DAY = 86_400_000_000_000L;

  private Canonical() {}

  /** The canonical lines of every row of a Parquet file, in file order. */
  public static List<String> lines(Path file) throws IOException {
    List<String> out = new ArrayList<>();
    try (TparquetReader r = TparquetReader.open(file)) {
      for (Group g = r.read(); g != null; g = r.read()) {
        out.add(line(g));
      }
    }
    return out;
  }

  /** The canonical line of one row (without the newline). */
  public static String line(Group row) {
    StringBuilder sb = new StringBuilder();
    struct(sb, row);
    return sb.toString();
  }

  private static void struct(StringBuilder sb, Group g) {
    GroupType t = g.getType();
    sb.append('{');
    for (int i = 0; i < t.getFieldCount(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(string(t.getFieldName(i))).append(':');
      field(sb, g, i);
    }
    sb.append('}');
  }

  /** Field {@code i} of {@code g}: a value, {@code null}, or (repeated, outside a LIST) an array. */
  private static void field(StringBuilder sb, Group g, int i) {
    Type t = g.getType().getType(i);
    int n = g.getFieldRepetitionCount(i);
    if (t.isRepetition(Type.Repetition.REPEATED)) {
      sb.append('[');
      for (int j = 0; j < n; j++) {
        if (j > 0) {
          sb.append(',');
        }
        value(sb, t, g, i, j);
      }
      sb.append(']');
    } else if (n == 0) {
      sb.append("null");
    } else {
      value(sb, t, g, i, 0);
    }
  }

  private static void value(StringBuilder sb, Type t, Group g, int i, int j) {
    if (t.isPrimitive()) {
      primitive(sb, t.asPrimitiveType(), g, i, j);
      return;
    }
    Group v = g.getGroup(i, j);
    LogicalTypeAnnotation a = t.getLogicalTypeAnnotation();
    if (a instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      list(sb, v);
    } else if (a instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
        || a instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
      map(sb, v);
    } else {
      struct(sb, v);
    }
  }

  /**
   * A LIST group, by the backward-compatibility rules of the Parquet format (LogicalTypes.md):
   * the repeated field is the element when it is a primitive, a group of several fields, or a
   * group named {@code array} or {@code <list>_tuple}; otherwise its only field is.
   */
  private static void list(StringBuilder sb, Group list) {
    GroupType lt = list.getType();
    sb.append('[');
    if (lt.getFieldCount() == 1) {
      Type repeated = lt.getType(0);
      int n = list.getFieldRepetitionCount(0);
      boolean elementIsRepeated = repeated.isPrimitive()
          || repeated.asGroupType().getFieldCount() != 1
          || repeated.getName().equals("array")
          || repeated.getName().equals(lt.getName() + "_tuple");
      for (int j = 0; j < n; j++) {
        if (j > 0) {
          sb.append(',');
        }
        if (elementIsRepeated) {
          value(sb, repeated, list, 0, j);
        } else {
          field(sb, list.getGroup(0, j), 0);
        }
      }
    }
    sb.append(']');
  }

  /** A MAP group: its key-value entries as objects with the entry's field names, in stored order. */
  private static void map(StringBuilder sb, Group map) {
    sb.append('[');
    int n = map.getType().getFieldCount() == 0 ? 0 : map.getFieldRepetitionCount(0);
    for (int j = 0; j < n; j++) {
      if (j > 0) {
        sb.append(',');
      }
      struct(sb, map.getGroup(0, j));
    }
    sb.append(']');
  }

  private static void primitive(StringBuilder sb, PrimitiveType t, Group g, int i, int j) {
    LogicalTypeAnnotation a = t.getLogicalTypeAnnotation();
    switch (t.getPrimitiveTypeName()) {
      case BOOLEAN:
        sb.append(g.getBoolean(i, j));
        break;
      case INT32:
        int v32 = g.getInteger(i, j);
        sb.append(unsigned(a) ? Integer.toUnsignedString(v32) : Integer.toString(v32));
        break;
      case INT64:
        long v64 = g.getLong(i, j);
        sb.append(unsigned(a) ? Long.toUnsignedString(v64) : Long.toString(v64));
        break;
      case FLOAT:
        sb.append(String.format("\"0x%08x\"", Float.floatToRawIntBits(g.getFloat(i, j))));
        break;
      case DOUBLE:
        sb.append(String.format("\"0x%016x\"", Double.doubleToRawLongBits(g.getDouble(i, j))));
        break;
      case INT96:
        sb.append(int96Nanos(g.getInt96(i, j)));
        break;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        binary(sb, a, g.getBinary(i, j));
        break;
      default:
        throw new IllegalStateException("unknown physical type " + t);
    }
  }

  private static boolean unsigned(LogicalTypeAnnotation a) {
    return a instanceof IntLogicalTypeAnnotation && !((IntLogicalTypeAnnotation) a).isSigned();
  }

  private static void binary(StringBuilder sb, LogicalTypeAnnotation a, Binary b) {
    if (a instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
        || a instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
        || a instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
      sb.append(string(b.toStringUsingUTF8()));
    } else if (a instanceof DecimalLogicalTypeAnnotation) {
      sb.append(new BigInteger(b.getBytes()));
    } else if (a instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
      byte[] h = b.getBytes();
      sb.append(String.format("\"0x%04x\"", (h[1] & 0xff) << 8 | (h[0] & 0xff)));
    } else {
      sb.append('"').append(Base64.getEncoder().encodeToString(b.getBytes())).append('"');
    }
  }

  /** An INT96 timestamp (nanoseconds of the day, then the Julian day) in nanoseconds since the epoch. */
  static long int96Nanos(Binary b) {
    ByteBuffer buf = ByteBuffer.wrap(b.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
    long nanos = buf.getLong();
    long day = Integer.toUnsignedLong(buf.getInt());
    return (day - JULIAN_EPOCH_DAY) * NANOS_PER_DAY + nanos;
  }

  /** A JSON string: {@code "} {@code \} and the control characters escaped, everything else as itself. */
  public static String string(String s) {
    StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
    for (int k = 0; k < s.length(); k++) {
      char c = s.charAt(k);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\f':
          sb.append("\\f");
          break;
        case '\r':
          sb.append("\\r");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.append('"').toString();
  }
}
