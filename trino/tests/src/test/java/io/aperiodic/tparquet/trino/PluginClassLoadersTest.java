package io.aperiodic.tparquet.trino;

import io.aperiodic.tparquet.Pcodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Trino gives each plugin, and each catalog, its own class loader, so a server with PCODEC in
 * several connectors (or catalogs) loads tparquet-jni several times. Each copy extracts the native
 * library bundled in its jar to a file of its own, which the JVM allows; one library file can be
 * loaded by one class loader only, so the tparquet.jni.library property, which names one file, is
 * for a single class loader (trino/README.md).
 */
class PluginClassLoadersTest
{
    @TempDir
    static Path tmp;
    static Path hive;
    String property;

    @BeforeEach
    void install()
            throws Exception
    {
        if (hive == null) {
            Path home = TrinoInstall.home(tmp.resolve("trino"));
            TrinoInstall.install(home);
            hive = home.resolve("plugin").resolve("hive");
        }
        property = System.getProperty("tparquet.jni.library");
    }

    @AfterEach
    void restore()
    {
        if (property != null) {
            System.setProperty("tparquet.jni.library", property);
        }
    }

    /** Compresses and decompresses with the plugin's copy of the codec. */
    static byte[] roundTrip(ClassLoader plugin, byte[] data)
            throws Exception
    {
        Class<?> pcodec = Class.forName("io.aperiodic.tparquet.Pcodec", true, plugin);
        assertThat(pcodec).isNotSameAs(Pcodec.class);
        Class<?> physicalType = Class.forName("io.aperiodic.tparquet.PhysicalType", true, plugin);
        Object int64 = physicalType.getMethod("valueOf", String.class).invoke(null, "INT64");
        Method compress = pcodec.getMethod("compress", byte[].class, physicalType);
        Method decompress = pcodec.getMethod("decompress", byte[].class, int.class);
        byte[] payload = (byte[]) compress.invoke(null, data, int64);
        return (byte[]) decompress.invoke(null, payload, data.length);
    }

    @Test
    void twoClassLoadersOfTheInstalledPluginLoadTheBundledLibrary()
            throws Exception
    {
        // this JVM's own copy (the test class path) has loaded the library already
        assertThat(Pcodec.compressBound(8)).isEqualTo(10);
        Pcodec.compress(new byte[16], null);
        System.clearProperty("tparquet.jni.library");
        byte[] data = new byte[80_000];
        new Random(3).nextBytes(data);
        ClassLoader catalog1 = TrinoInstall.pluginClassLoader(hive);
        ClassLoader catalog2 = TrinoInstall.pluginClassLoader(hive);
        assertThat(roundTrip(catalog1, data)).isEqualTo(data);
        assertThat(roundTrip(catalog2, data)).isEqualTo(data);
    }

    @Test
    void theLibraryPropertyIsForOneClassLoader()
            throws Exception
    {
        Path library = Path.of(property);
        assertThat(Files.exists(library)).isTrue();
        // two catalogs' class loaders, both told to load the same file: at most one can
        int loaded = 0;
        for (ClassLoader catalog : new ClassLoader[] {TrinoInstall.pluginClassLoader(hive), TrinoInstall.pluginClassLoader(hive)}) {
            try {
                roundTrip(catalog, new byte[8]);
                loaded++;
            }
            catch (InvocationTargetException | LinkageError e) {
                Throwable root = e;
                while (root.getCause() != null) {
                    root = root.getCause();
                }
                assertThat(root).isInstanceOf(UnsatisfiedLinkError.class).hasMessageContaining("already loaded in another classloader");
            }
        }
        assertThat(loaded).isLessThanOrEqualTo(1);
    }
}
