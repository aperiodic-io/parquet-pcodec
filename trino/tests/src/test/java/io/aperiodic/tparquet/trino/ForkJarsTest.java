package io.aperiodic.tparquet.trino;

import io.trino.parquet.ParquetCompressionUtils;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Trino's reader runs on the forks: the parquet-java 1.18.1 fork's jars (thrift relocated into
 * format-structures, as upstream's), with upstream's parquet-column 1.18.1.
 */
class ForkJarsTest
{
    static String jar(Class<?> c)
            throws URISyntaxException
    {
        return Path.of(c.getProtectionDomain().getCodeSource().getLocation().toURI()).getFileName().toString();
    }

    @Test
    void theParquetJavaJarsAreThe1181Fork()
            throws Exception
    {
        assertThat(jar(CompressionCodec.class)).isEqualTo("parquet-format-structures-1.18.1-tparquet.jar");
        assertThat(jar(CompressionCodecName.class)).isEqualTo("parquet-common-1.18.1-tparquet.jar");
        assertThat(jar(org.apache.parquet.column.ColumnDescriptor.class)).isEqualTo("parquet-column-1.18.1.jar");
        assertThat(CompressionCodecName.fromParquet(CompressionCodec.PCODEC)).isSameAs(CompressionCodecName.PCODEC);
        assertThat(org.apache.parquet.Version.VERSION_NUMBER).isEqualTo("1.18.1");
    }

    @Test
    void thriftIsRelocatedIntoFormatStructures()
            throws Exception
    {
        // the generated classes refer to the relocated thrift, which the jar holds
        assertThat(CompressionCodec.class.getInterfaces()).extracting(Class::getName).containsExactly("shaded.parquet.org.apache.thrift.TEnum");
        Class<?> base = Class.forName("shaded.parquet.org.apache.thrift.TBase");
        assertThat(jar(base)).isEqualTo("parquet-format-structures-1.18.1-tparquet.jar");
        assertThat(Util.class.getDeclaredMethods()).isNotEmpty();
        // classes major version 55: Java 11, as upstream
        try (var in = CompressionCodec.class.getResourceAsStream("CompressionCodec.class")) {
            byte[] header = in.readNBytes(8);
            assertThat(((header[6] & 0xff) << 8) | (header[7] & 0xff)).isEqualTo(55);
        }
    }

    @Test
    void theReaderIsTheTrinoParquetFork()
    {
        // the reactor's trino-parquet (target/classes or its jar), with the fork's classes
        assertThat(ParquetCompressionUtils.class.getResource("PcodecPages.class")).isNotNull();
        assertThat(ParquetCompressionUtils.class.getResource("writer/PcodecColumns.class")).isNotNull();
    }
}
