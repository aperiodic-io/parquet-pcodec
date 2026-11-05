package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

/** Canonical JSON (testdata/README.md) of single rows. */
class CanonicalTest {
  static String line(String schema, java.util.function.Consumer<Group> fill) {
    MessageType t = MessageTypeParser.parseMessageType(schema);
    Group g = new SimpleGroupFactory(t).newGroup();
    fill.accept(g);
    return Canonical.line(g);
  }

  @Test
  void stringsAreEscapedAsSpecified() {
    String s = "q\"b\\s/\b\t\n\f\r\u0001\u001f\u007fü€😀";
    assertThat(line("message m { required binary s (STRING); }", g -> g.append("s", s)))
        .isEqualTo("{\"s\":\"q\\\"b\\\\s/\\b\\t\\n\\f\\r\\u0001\\u001f\u007fü€😀\"}");
    assertThat(Canonical.string("")).isEqualTo("\"\"");
  }

  @Test
  void enumsAndJsonAreStrings() {
    assertThat(line("message m { required binary e (ENUM); required binary j (JSON); }",
        g -> g.append("e", "A").append("j", "{\"x\":1}"))).isEqualTo("{\"e\":\"A\",\"j\":\"{\\\"x\\\":1}\"}");
  }

  @Test
  void floatsAreTheirBitsInHex() {
    assertThat(line("message m { required double d; required float f; }",
        g -> g.append("d", 1.0).append("f", 1.0f))).isEqualTo("{\"d\":\"0x3ff0000000000000\",\"f\":\"0x3f800000\"}");
    assertThat(line("message m { required double d; required float f; }",
        g -> g.append("d", -0.0).append("f", Float.intBitsToFloat(0x7fc00001))))
        .isEqualTo("{\"d\":\"0x8000000000000000\",\"f\":\"0x7fc00001\"}");
    assertThat(line("message m { required double d; }", g -> g.append("d", Double.longBitsToDouble(1))))
        .isEqualTo("{\"d\":\"0x0000000000000001\"}");
  }

  @Test
  void halfFloatsAreFourHexDigitsOfTheirLittleEndianBits() {
    assertThat(line("message m { required fixed_len_byte_array(2) h (FLOAT16); }",
        g -> g.append("h", Binary.fromConstantByteArray(new byte[] {0x00, 0x3c})))).isEqualTo("{\"h\":\"0x3c00\"}");
  }

  @Test
  void integersOfEveryKindArePlainNumbers() {
    assertThat(line(String.join(" ", "message m {",
        "required int32 a; required int64 b; required int32 u8 (INTEGER(8,false)); required int32 u32 (INTEGER(32,false));",
        "required int64 u64 (INTEGER(64,false)); required int32 i8 (INTEGER(8,true)); required int32 d (DATE);",
        "required int64 ts (TIMESTAMP(NANOS,true)); required int32 t (TIME(MILLIS,false)); required int64 dec (DECIMAL(18,4)); }"),
        g -> g.append("a", -12).append("b", 1735689600000L).append("u8", 255).append("u32", -1).append("u64", -1L)
            .append("i8", -128).append("d", 20089).append("ts", -1L).append("t", 1000).append("dec", -1234500L)))
        .isEqualTo("{\"a\":-12,\"b\":1735689600000,\"u8\":255,\"u32\":4294967295,\"u64\":18446744073709551615,"
            + "\"i8\":-128,\"d\":20089,\"ts\":-1,\"t\":1000,\"dec\":-1234500}");
  }

  @Test
  void binaryDecimalsAreTheirUnscaledInteger() {
    assertThat(line("message m { required fixed_len_byte_array(16) d (DECIMAL(38,2)); required binary e (DECIMAL(20,0)); }",
        g -> g.append("d", Binary.fromConstantByteArray(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -2}))
            .append("e", Binary.fromConstantByteArray(new byte[] {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))))
        .isEqualTo("{\"d\":-2,\"e\":18446744073709551616}");
  }

  @Test
  void binaryIsBase64() {
    assertThat(line("message m { required binary b; required fixed_len_byte_array(3) f; }",
        g -> g.append("b", Binary.fromConstantByteArray(new byte[] {(byte) 0xfb, (byte) 0xff})).append("f", Binary.fromString("abc"))))
        .isEqualTo("{\"b\":\"+/8=\",\"f\":\"YWJj\"}");
  }

  @Test
  void int96IsTheTimestampInNanoseconds() {
    // 2000-01-01T00:00:00.000000001: Julian day 2451545, 1 ns into the day
    byte[] b = new byte[12];
    b[0] = 1;
    int day = 2451545;
    b[8] = (byte) day;
    b[9] = (byte) (day >> 8);
    b[10] = (byte) (day >> 16);
    b[11] = (byte) (day >> 24);
    assertThat(line("message m { required int96 t; }", g -> g.add("t", Binary.fromConstantByteArray(b))))
        .isEqualTo("{\"t\":946684800000000001}");
  }

  @Test
  void booleansAndNulls() {
    assertThat(line("message m { required boolean a; optional boolean b; optional group s { optional int32 x; } }",
        g -> g.append("a", true))).isEqualTo("{\"a\":true,\"b\":null,\"s\":null}");
  }

  @Test
  void structsKeepTheirFieldOrder() {
    assertThat(line("message m { required group s { optional int32 z; required int32 a; } }",
        g -> g.addGroup("s").append("a", 1))).isEqualTo("{\"s\":{\"z\":null,\"a\":1}}");
  }

  @Test
  void listsInEveryLayout() {
    // the standard three levels, an empty list, null elements
    assertThat(line("message m { optional group l (LIST) { repeated group list { optional int64 element; } } }", g -> {
      Group l = g.addGroup("l");
      l.addGroup("list").append("element", 1L);
      l.addGroup("list");
    })).isEqualTo("{\"l\":[1,null]}");
    assertThat(line("message m { optional group l (LIST) { repeated group list { optional int64 element; } } }",
        g -> g.addGroup("l"))).isEqualTo("{\"l\":[]}");
    // two levels: a repeated primitive, and a repeated group named "array" (legacy writers)
    assertThat(line("message m { required group l (LIST) { repeated int32 element; } }", g -> {
      Group l = g.addGroup("l");
      l.append("element", 3).append("element", 4);
    })).isEqualTo("{\"l\":[3,4]}");
    assertThat(line("message m { required group l (LIST) { repeated group array { required int32 x; } } }", g -> {
      g.addGroup("l").addGroup("array").append("x", 5);
    })).isEqualTo("{\"l\":[{\"x\":5}]}");
    // a repeated field outside a LIST is a list of its values
    assertThat(line("message m { repeated int32 r; }", g -> g.append("r", 1).append("r", 2))).isEqualTo("{\"r\":[1,2]}");
    assertThat(line("message m { repeated int32 r; }", g -> {})).isEqualTo("{\"r\":[]}");
    // a list of lists
    assertThat(line("message m { optional group l (LIST) { repeated group list { optional group element (LIST) {"
        + " repeated group list { optional int32 element; } } } } }", g -> {
          Group inner = g.addGroup("l").addGroup("list").addGroup("element");
          inner.addGroup("list").append("element", 7);
        })).isEqualTo("{\"l\":[[7]]}");
  }

  @Test
  void mapsAreEntryObjectsInStoredOrder() {
    assertThat(line("message m { optional group m (MAP) { repeated group key_value { required binary key (STRING);"
        + " optional int32 value; } } }", g -> {
          Group m = g.addGroup("m");
          m.addGroup("key_value").append("key", "b").append("value", 2);
          m.addGroup("key_value").append("key", "a");
        })).isEqualTo("{\"m\":[{\"key\":\"b\",\"value\":2},{\"key\":\"a\",\"value\":null}]}");
  }
}
