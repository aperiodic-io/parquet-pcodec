package io.aperiodic.tparquet.trino;

import io.trino.server.PluginManager;
import io.trino.spi.Plugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * A Trino install's layout around the Hive plugin directory of Trino 484 (built by
 * trino/plugin-jars), the install script, and the plugin loaded the way the server loads a plugin
 * directory: every jar of it in a PluginClassLoader (child first, the SPI from the server).
 */
final class TrinoInstall
{
    private TrinoInstall() {}

    static Path pluginJars()
    {
        return Path.of(System.getProperty("tparquet.trino.plugin.jars", "../plugin-jars/target"));
    }

    static Path script()
    {
        return Path.of(System.getProperty("tparquet.trino.install", "../install-tparquet.sh"));
    }

    /** The upstream Hive plugin directory, as Trino 484's server tarball has it. */
    static Path upstreamHivePlugin()
    {
        Path dir = pluginJars().resolve("plugin").resolve("hive");
        if (!Files.isDirectory(dir)) {
            throw new IllegalStateException(dir + " is missing: build trino/plugin-jars first (make -C trino test)");
        }
        return dir;
    }

    /** The jars the install script installs. */
    static Path tparquetJars()
    {
        return pluginJars().resolve("tparquet-jars");
    }

    /** A Trino home at {@code home} whose plugin/hive links every jar of the upstream Hive plugin. */
    static Path home(Path home)
            throws IOException
    {
        Path hive = Files.createDirectories(home.resolve("plugin").resolve("hive"));
        try (Stream<Path> jars = Files.list(upstreamHivePlugin())) {
            for (Path jar : jars.toList()) {
                Files.createSymbolicLink(hive.resolve(jar.getFileName()), jar.toAbsolutePath());
            }
        }
        return home;
    }

    /** The result of running the install script. */
    record Result(int exitCode, String output) {}

    static Result run(String... args)
    {
        List<String> command = new ArrayList<>(List.of("bash", script().toString()));
        command.addAll(List.of(args));
        try {
            Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!p.waitFor(2, TimeUnit.MINUTES)) {
                p.destroyForcibly();
                throw new IllegalStateException("install-tparquet.sh did not finish");
            }
            return new Result(p.exitValue(), out);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Installs the tparquet jars into {@code home}, failing unless the script succeeds. */
    static void install(Path home)
    {
        Result r = run("--jars", tparquetJars().toString(), home.toString());
        if (r.exitCode() != 0) {
            throw new IllegalStateException("install-tparquet.sh failed: " + r.output());
        }
    }

    /** A class loader over every jar of a plugin directory, as Trino's server creates one. */
    static ClassLoader pluginClassLoader(Path pluginDir)
            throws IOException
    {
        List<URL> urls = new ArrayList<>();
        try (Stream<Path> jars = Files.list(pluginDir)) {
            for (Path jar : jars.filter(p -> p.toString().endsWith(".jar")).sorted().toList()) {
                urls.add(url(jar));
            }
        }
        return PluginManager.createClassLoader(pluginDir.getFileName().toString(), urls);
    }

    private static URL url(Path jar)
    {
        try {
            return jar.toUri().toURL();
        }
        catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * The plugin of a plugin directory, from its class loader. The server finds it through
     * ServiceLoader; the trino-hive jar on Maven Central has no META-INF/services entry (Trino's
     * server tarball build generates it), so the class is named when ServiceLoader finds none.
     */
    static Plugin plugin(Path pluginDir, String pluginClass)
            throws IOException
    {
        ClassLoader loader = pluginClassLoader(pluginDir);
        List<Plugin> plugins = ServiceLoader.load(Plugin.class, loader).stream().map(ServiceLoader.Provider::get).toList();
        if (plugins.size() > 1) {
            throw new IllegalStateException(pluginDir + " has " + plugins.size() + " plugins");
        }
        if (plugins.size() == 1) {
            return plugins.getFirst();
        }
        try {
            return (Plugin) Class.forName(pluginClass, true, loader).getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    static Plugin hivePlugin(Path home)
            throws IOException
    {
        return plugin(home.resolve("plugin").resolve("hive"), "io.trino.plugin.hive.HivePlugin");
    }
}
