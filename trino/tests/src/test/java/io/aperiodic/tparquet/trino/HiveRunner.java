package io.aperiodic.tparquet.trino;

import com.google.inject.Key;
import io.trino.Session;
import io.trino.metadata.HandleResolver;
import io.trino.server.PluginClassLoader;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * A Trino query engine (DistributedQueryRunner, one node) with a Hive catalog over local files: the
 * file metastore and Trino's local file system, rooted at a data directory whose subdirectories
 * are reachable as {@code local:///<dir>}.
 */
final class HiveRunner
        implements AutoCloseable
{
    static final String CATALOG = "hive";
    static final String SCHEMA = "tparquet";

    private final DistributedQueryRunner runner;
    private final Session session;

    HiveRunner(Plugin hivePlugin, Path data)
            throws Exception
    {
        this.session = testSessionBuilder().setCatalog(CATALOG).setSchema(SCHEMA).build();
        this.runner = DistributedQueryRunner.builder(session).setWorkerCount(0).build();
        try {
            // as the server's PluginManager loads a plugin directory: the class loader's handles
            // (serialized between coordinator and workers by class loader id), then the plugin
            if (hivePlugin.getClass().getClassLoader() instanceof PluginClassLoader loader) {
                for (TestingTrinoServer server : runner.getServers()) {
                    server.getInstance(Key.get(HandleResolver.class)).registerClassLoader(loader);
                }
            }
            runner.installPlugin(hivePlugin);
            runner.createCatalog(CATALOG, "hive", Map.of(
                    "hive.metastore", "file",
                    "hive.metastore.catalog.dir", "local:///metastore",
                    "fs.local.enabled", "true",
                    "local.location", data.toAbsolutePath().toString(),
                    // TIMESTAMP(NANOS) columns, read at their precision
                    "hive.timestamp-precision", "NANOSECONDS"));
            execute("CREATE SCHEMA " + SCHEMA + " WITH (location = 'local:///schema')");
        }
        catch (Exception e) {
            runner.close();
            throw e;
        }
    }

    MaterializedResult execute(String sql)
    {
        return runner.execute(session, sql);
    }

    List<MaterializedRow> rows(String sql)
    {
        return execute(sql).getMaterializedRows();
    }

    Object value(String sql)
    {
        return execute(sql).getOnlyValue();
    }

    /** The rows a query read from its tables (physical input positions), after running it. */
    long inputRows(String sql)
    {
        return inputRows(sql, Map.of());
    }

    /** As {@link #inputRows(String)}, with Hive session properties. */
    long inputRows(String sql, Map<String, String> hiveSessionProperties)
    {
        Session.SessionBuilder s = Session.builder(session);
        hiveSessionProperties.forEach((k, v) -> s.setCatalogSessionProperty(CATALOG, k, v));
        QueryId id = runner.executeWithPlan(s.build(), sql).queryId();
        return runner.getCoordinator().getQueryManager().getFullQueryInfo(id).getQueryStats().getPhysicalInputPositions();
    }

    /** An external Parquet table over {@code local:///<dir>}. */
    void table(String name, String columns, String dir)
    {
        execute("CREATE TABLE " + name + " (" + columns + ") WITH (format = 'PARQUET', external_location = 'local:///" + dir + "')");
    }

    @Override
    public void close()
    {
        runner.close();
    }
}
