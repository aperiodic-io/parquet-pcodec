package io.aperiodic.tparquet.trino;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** install-tparquet.sh on directories laid out like a Trino 484 install, and on the real Hive plugin directory. */
class InstallScriptTest
{
    static final List<String> FORK_JARS = List.of(
            "trino-parquet-484-tparquet.jar", "parquet-format-structures-1.18.1-tparquet.jar", "parquet-common-1.18.1-tparquet.jar");
    static final List<String> UPSTREAM_JARS = List.of(
            "trino-parquet-484.jar", "parquet-format-structures-1.18.1.jar", "parquet-common-1.18.1.jar");

    @TempDir
    Path tmp;

    /** A Trino home: the connectors with trino-parquet (and their other jars), and two without. */
    static Path fakeHome(Path home)
            throws IOException
    {
        for (String plugin : List.of("hive", "iceberg", "delta-lake", "hudi")) {
            Path dir = Files.createDirectories(home.resolve("plugin").resolve(plugin));
            for (String jar : List.of("trino-" + plugin + "-484.jar", "trino-parquet-484.jar", "parquet-format-structures-1.18.1.jar",
                    "parquet-common-1.18.1.jar", "parquet-column-1.18.1.jar", "parquet-encoding-1.18.1.jar", "guava-33.5.0-jre.jar")) {
                Files.writeString(dir.resolve(jar), plugin + "/" + jar);
            }
        }
        for (String plugin : List.of("tpch", "memory")) {
            Path dir = Files.createDirectories(home.resolve("plugin").resolve(plugin));
            Files.writeString(dir.resolve("trino-" + plugin + "-484.jar"), plugin);
        }
        Files.createDirectories(home.resolve("lib"));
        Files.writeString(home.resolve("lib").resolve("trino-main-484.jar"), "main");
        return home;
    }

    /** Every file under {@code dir} with its content, as a sorted map of relative paths. */
    static Map<String, String> tree(Path dir)
    {
        Map<String, String> out = new TreeMap<>();
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) {
                byte[] bytes = Files.readAllBytes(p);
                out.put(dir.relativize(p).toString(), bytes.length < 200 ? new String(bytes, StandardCharsets.UTF_8) : bytes.length + " bytes");
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    static List<String> names(Path dir)
            throws IOException
    {
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    static TrinoInstall.Result install(Path home)
    {
        return TrinoInstall.run("--jars", TrinoInstall.tparquetJars().toString(), home.toString());
    }

    static String jniJar()
            throws IOException
    {
        return names(TrinoInstall.tparquetJars()).stream().filter(n -> n.startsWith("tparquet-jni-")).findFirst().orElseThrow();
    }

    @Test
    void installsIntoEveryConnectorWithTrinoParquet()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        Map<String, String> before = tree(home);
        TrinoInstall.Result r = install(home);
        assertThat(r.exitCode()).as(r.output()).isZero();
        for (String plugin : List.of("hive", "iceberg", "delta-lake", "hudi")) {
            assertThat(r.output()).contains("install-tparquet: " + plugin + ": ");
            Path dir = home.resolve("plugin").resolve(plugin);
            assertThat(names(dir)).containsAll(FORK_JARS).contains(jniJar()).doesNotContainAnyElementsOf(UPSTREAM_JARS)
                    // the connector's other jars stay
                    .contains("trino-" + plugin + "-484.jar", "parquet-column-1.18.1.jar", "parquet-encoding-1.18.1.jar", "guava-33.5.0-jre.jar")
                    .hasSize(8);
            for (String jar : FORK_JARS) {
                assertThat(dir.resolve(jar)).hasSameBinaryContentAs(TrinoInstall.tparquetJars().resolve(jar));
            }
            // upstream's jars are kept outside the plugin directory (Trino loads every jar in it)
            Path backup = home.resolve("tparquet-backup").resolve(plugin);
            assertThat(names(backup)).containsExactlyInAnyOrderElementsOf(UPSTREAM_JARS);
            assertThat(backup.resolve("trino-parquet-484.jar")).hasContent(plugin + "/trino-parquet-484.jar");
        }
        // the connectors without trino-parquet and the server are untouched
        Map<String, String> after = tree(home);
        for (String path : List.of("plugin/tpch/trino-tpch-484.jar", "plugin/memory/trino-memory-484.jar", "lib/trino-main-484.jar")) {
            assertThat(after.get(path)).isEqualTo(before.get(path));
        }
        assertThat(names(home.resolve("plugin").resolve("tpch"))).containsExactly("trino-tpch-484.jar");
        assertThat(r.output()).doesNotContain("tpch", "memory");
    }

    @Test
    void runningItAgainUpdatesTheInstalledJars()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        assertThat(install(home).exitCode()).isZero();
        Map<String, String> once = tree(home);
        // an older tparquet-jni is replaced, not kept next to the new one
        Path hive = home.resolve("plugin").resolve("hive");
        Files.move(hive.resolve(jniJar()), hive.resolve("tparquet-jni-0.0.1.jar"));
        TrinoInstall.Result again = install(home);
        assertThat(again.exitCode()).as(again.output()).isZero();
        assertThat(tree(home)).isEqualTo(once);
    }

    @Test
    void uninstallRestoresUpstream()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        Map<String, String> before = tree(home);
        assertThat(install(home).exitCode()).isZero();
        TrinoInstall.Result r = TrinoInstall.run("--uninstall", home.toString());
        assertThat(r.exitCode()).as(r.output()).isZero();
        assertThat(r.output()).contains("install-tparquet: hive: upstream jars restored");
        assertThat(tree(home)).isEqualTo(before);
        assertThat(home.resolve("tparquet-backup")).doesNotExist();
        // and again: nothing to restore
        TrinoInstall.Result again = TrinoInstall.run("--uninstall", home.toString());
        assertThat(again.exitCode()).isZero();
        assertThat(again.output()).contains("hive: not installed");
        assertThat(tree(home)).isEqualTo(before);
    }

    @Test
    void refusesAnotherTrinoVersionWithoutChangingAnything()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        Path iceberg = home.resolve("plugin").resolve("iceberg");
        Files.move(iceberg.resolve("trino-parquet-484.jar"), iceberg.resolve("trino-parquet-485.jar"));
        Map<String, String> before = tree(home);
        TrinoInstall.Result r = install(home);
        assertThat(r.exitCode()).isEqualTo(1);
        assertThat(r.output()).contains("iceberg: trino-parquet-485.jar is not trino-parquet 484 (this is for Trino 484)");
        // not even the directories that were fine
        assertThat(tree(home)).isEqualTo(before);
    }

    @Test
    void refusesAnotherParquetVersion()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        Path hudi = home.resolve("plugin").resolve("hudi");
        Files.move(hudi.resolve("parquet-common-1.18.1.jar"), hudi.resolve("parquet-common-1.17.0.jar"));
        Map<String, String> before = tree(home);
        TrinoInstall.Result r = install(home);
        assertThat(r.exitCode()).isEqualTo(1);
        assertThat(r.output()).contains("hudi: parquet-common-1.17.0.jar is not parquet-common 1.18.1");
        assertThat(tree(home)).isEqualTo(before);
        Files.delete(hudi.resolve("parquet-common-1.17.0.jar"));
        TrinoInstall.Result missing = install(home);
        assertThat(missing.exitCode()).isEqualTo(1);
        assertThat(missing.output()).contains("hudi: no parquet-common jar (expected parquet-common-1.18.1.jar)");
    }

    @Test
    void refusesTwoTrinoParquetJars()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        Files.writeString(home.resolve("plugin").resolve("hive").resolve("trino-parquet-484-tparquet.jar"), "stray");
        Map<String, String> before = tree(home);
        TrinoInstall.Result r = install(home);
        assertThat(r.exitCode()).isEqualTo(1);
        assertThat(r.output()).contains("has 2 jars matching trino-parquet-*.jar");
        assertThat(tree(home)).isEqualTo(before);
    }

    @Test
    void refusesAForkJarWithoutItsBackup()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        assertThat(install(home).exitCode()).isZero();
        Files.delete(home.resolve("tparquet-backup").resolve("delta-lake").resolve("parquet-common-1.18.1.jar"));
        TrinoInstall.Result r = install(home);
        assertThat(r.exitCode()).isEqualTo(1);
        assertThat(r.output()).contains("delta-lake: parquet-common-1.18.1-tparquet.jar without a backup of parquet-common-1.18.1.jar");
    }

    @Test
    void refusesWhatIsNotATrinoInstall()
            throws IOException
    {
        TrinoInstall.Result none = install(tmp);
        assertThat(none.exitCode()).isEqualTo(1);
        assertThat(none.output()).contains("is not a Trino install (no plugin directory)");
        Path home = tmp.resolve("trino");
        Files.createDirectories(home.resolve("plugin").resolve("tpch"));
        TrinoInstall.Result noParquet = install(home);
        assertThat(noParquet.exitCode()).isEqualTo(1);
        assertThat(noParquet.output()).contains("no plugin directory of " + home + " holds trino-parquet");
    }

    @Test
    void refusesAJarDirectoryWithoutTheJars()
            throws IOException
    {
        Path home = fakeHome(tmp.resolve("trino"));
        Map<String, String> before = tree(home);
        TrinoInstall.Result missing = TrinoInstall.run("--jars", tmp.resolve("nowhere").toString(), home.toString());
        assertThat(missing.exitCode()).isEqualTo(1);
        assertThat(missing.output()).contains("no jar directory");
        Path partial = Files.createDirectories(tmp.resolve("partial"));
        Files.copy(TrinoInstall.tparquetJars().resolve("trino-parquet-484-tparquet.jar"), partial.resolve("trino-parquet-484-tparquet.jar"));
        TrinoInstall.Result incomplete = TrinoInstall.run("--jars", partial.toString(), home.toString());
        assertThat(incomplete.exitCode()).isEqualTo(1);
        assertThat(incomplete.output()).contains("must hold trino-parquet-484-tparquet.jar");
        assertThat(tree(home)).isEqualTo(before);
    }

    @Test
    void usage()
    {
        for (String[] args : new String[][] {{}, {"a", "b"}, {"--jars"}, {"--frobnicate", "x"}}) {
            TrinoInstall.Result r = TrinoInstall.run(args);
            assertThat(r.exitCode()).as(String.join(" ", args)).isEqualTo(2);
            assertThat(r.output()).contains("usage:");
        }
    }

    @Test
    void installsIntoTheRealHivePluginDirectory()
            throws IOException
    {
        Path home = TrinoInstall.home(tmp.resolve("trino"));
        Path hive = home.resolve("plugin").resolve("hive");
        List<String> upstream = names(hive);
        assertThat(upstream).containsAll(UPSTREAM_JARS).contains("trino-hive-484.jar", "parquet-column-1.18.1.jar").hasSizeGreaterThan(100);
        TrinoInstall.install(home);
        List<String> installed = names(hive);
        String jni = jniJar();
        assertThat(installed).containsAll(FORK_JARS).contains(jni).doesNotContainAnyElementsOf(UPSTREAM_JARS);
        // exactly: upstream's jars, the three replaced and tparquet-jni added
        assertThat(installed.stream().filter(n -> !FORK_JARS.contains(n) && !n.equals(jni)).toList())
                .containsExactlyElementsOf(upstream.stream().filter(n -> !UPSTREAM_JARS.contains(n)).toList());
        assertThat(TrinoInstall.run("--uninstall", home.toString()).exitCode()).isZero();
        assertThat(names(hive)).isEqualTo(upstream);
    }
}
