package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;

/** The fork's parquet-format-structures jar is shaded as upstream's (when tested as a jar: mvn verify). */
class TestPcodecShadedJar {
  @Test
  void thriftIsRelocated() throws Exception {
    String from = CompressionCodec.class.getProtectionDomain().getCodeSource().getLocation().toString();
    assumeTrue(from.endsWith(".jar"), "parquet-format-structures comes from " + from + ", not its jar (run mvn verify)");
    assertThat(CompressionCodec.class.getInterfaces()).extracting(Class::getName)
        .containsExactly("shaded.parquet.org.apache.thrift.TEnum");
    try (JarFile jar = new JarFile(Path.of(CompressionCodec.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toFile())) {
      List<String> names = jar.stream().map(e -> e.getName()).collect(Collectors.toList());
      assertThat(names).contains("shaded/parquet/org/apache/thrift/protocol/TCompactProtocol.class", "parquet.thrift");
      assertThat(names).noneMatch(n -> n.startsWith("org/apache/thrift/"));
    }
  }
}
