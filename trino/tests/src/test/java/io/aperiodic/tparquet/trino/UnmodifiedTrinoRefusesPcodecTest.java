package io.aperiodic.tparquet.trino;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unmodified Trino 484 (the Hive plugin directory as released, before install-tparquet.sh) refuses
 * every PCODEC vector at the footer instead of returning wrong data (spec section 2), and reads the
 * standard control file.
 */
class UnmodifiedTrinoRefusesPcodecTest
{
    @TempDir
    static Path tmp;
    static HiveRunner trino;

    @BeforeAll
    static void start()
            throws Exception
    {
        Path home = TrinoInstall.home(tmp.resolve("trino"));
        Path data = Files.createDirectories(tmp.resolve("data"));
        for (String name : Testdata.vectorNames()) {
            Files.copy(Testdata.file(name), Files.createDirectories(data.resolve(name)).resolve(name + ".parquet"));
        }
        trino = new HiveRunner(TrinoInstall.hivePlugin(home), data);
        for (String name : Testdata.vectorNames()) {
            trino.table("v_" + name, TrinoQueriesTest.COLUMNS.get(name), name);
        }
    }

    @AfterAll
    static void stop()
    {
        if (trino != null) {
            trino.close();
        }
    }

    static List<String> pcodecVectors()
    {
        return Testdata.pcodecVectorNames();
    }

    @Test
    void thisIsUpstreamTrinoParquet()
            throws Exception
    {
        Path dir = tmp.resolve("trino").resolve("plugin").resolve("hive");
        ClassLoader plugin = TrinoInstall.pluginClassLoader(dir);
        Class<?> codec = Class.forName("org.apache.parquet.format.CompressionCodec", false, plugin);
        assertThat(Path.of(codec.getProtectionDomain().getCodeSource().getLocation().toURI())).isEqualTo(dir.resolve("parquet-format-structures-1.18.1.jar"));
        assertThat(codec.getMethod("findByValue", int.class).invoke(null, 20547)).isNull();
        assertThat(pcodecVectors()).hasSizeGreaterThanOrEqualTo(12);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pcodecVectors")
    void refusesEveryPcodecVector(String name)
    {
        // thrift reads the unknown codec as null and refuses the column chunk metadata
        assertThatThrownBy(() -> trino.execute("SELECT count(*), count(" + firstColumn(name) + ") FROM v_" + name))
                .hasStackTraceContaining("Required field 'codec' was not present!");
    }

    private static String firstColumn(String name)
    {
        return TrinoQueriesTest.COLUMNS.get(name).split(" ", 2)[0];
    }

    @Test
    void readsTheStandardControl()
    {
        assertThat(trino.value("SELECT count(*) FROM v_standard_control")).isEqualTo(1500L);
        assertThat(trino.value("SELECT count(DISTINCT symbol) FROM v_standard_control")).isNotEqualTo(0L);
    }
}
