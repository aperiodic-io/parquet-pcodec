package io.aperiodic.tparquet.trino.upstream;

import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unmodified trino-parquet 484 refuses every PCODEC file instead of returning wrong data (spec section 2). */
class UpstreamTrinoRefusesPcodecTest
{
    static Path dir()
    {
        return Path.of(System.getProperty("tparquet.testdata", "../../testdata")).resolve("files");
    }

    static List<String> pcodecVectors()
    {
        try (Stream<Path> s = Files.list(dir())) {
            return s.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".parquet"))
                    .map(n -> n.substring(0, n.length() - ".parquet".length())).filter(n -> !n.equals("standard_control")).sorted().toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static final class InMemory
            extends AbstractParquetDataSource
    {
        private final byte[] data;

        InMemory(Path file)
                throws IOException
        {
            this(file, Files.readAllBytes(file));
        }

        private InMemory(Path file, byte[] data)
        {
            super(new ParquetDataSourceId(file.toString()), data.length, ParquetReaderOptions.defaultOptions());
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            System.arraycopy(data, (int) position, buffer, bufferOffset, bufferLength);
        }
    }

    static ParquetMetadata footer(String name)
            throws IOException
    {
        return MetadataReader.readFooter(new InMemory(dir().resolve(name + ".parquet")), Optional.empty());
    }

    @Test
    void thisIsUpstreamTrinoParquet()
            throws Exception
    {
        String jar = MetadataReader.class.getProtectionDomain().getCodeSource().getLocation().toString();
        assertThat(jar).endsWith("trino-parquet-484.jar");
        assertThat(MetadataReader.class.getClassLoader().getResource("io/trino/parquet/PcodecPages.class")).isNull();
        assertThat(org.apache.parquet.format.CompressionCodec.findByValue(20547)).isNull();
        assertThat(org.apache.parquet.format.CompressionCodec.class.getProtectionDomain().getCodeSource().getLocation().toString())
                .endsWith("parquet-format-structures-1.18.1.jar");
        assertThat(pcodecVectors()).hasSizeGreaterThanOrEqualTo(12);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pcodecVectors")
    void refusesEveryPcodecVector(String name)
    {
        // the footer's codec is unknown to thrift: the column chunk metadata is refused
        assertThatThrownBy(() -> footer(name)).hasStackTraceContaining("Required field 'codec' was not present!");
    }

    @Test
    void readsTheStandardControl()
            throws IOException
    {
        ParquetMetadata footer = footer("standard_control");
        assertThat(footer.getBlocks().stream().mapToLong(BlockMetadata::rowCount).sum()).isEqualTo(1500);
    }
}
