package bloomberg.presto.accumulo.benchmark;

import static java.lang.String.format;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.google.common.io.Files;

import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloSessionProperties;

public class TpchQueryExecutor {

    private static final Logger LOG = Logger.getLogger(TpchQueryExecutor.class);
    private static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String SCHEME = "jdbc:presto://";
    private static final String CATALOG = "accumulo";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static QueryMetrics run(AccumuloConfig accConfig, File qf,
            String host, int port, String schema,
            boolean optimizeColumnFiltersEnabled,
            boolean optimizeRangePredicatePushdownEnabled,
            boolean optimizeRangeSplitsEnabled, boolean secondaryIndexEnabled,
            int timeout) throws Exception {

        String dbUrl = String.format("%s%s:%d/%s/%s", SCHEME, host, port,
                CATALOG, schema);

        Properties props = new Properties();
        props.setProperty("user", "root");
        PrestoConnection conn = (PrestoConnection) DriverManager
                .getConnection(dbUrl, props);
        conn.setSessionProperty(
                AccumuloSessionProperties.OPTIMIZE_COLUMN_FILTERS_ENABLED,
                Boolean.toString(optimizeColumnFiltersEnabled));
        conn.setSessionProperty(
                AccumuloSessionProperties.OPTIMIZE_RANGE_PREDICATE_PUSHDOWN_ENABLED,
                Boolean.toString(optimizeRangePredicatePushdownEnabled));
        conn.setSessionProperty(
                AccumuloSessionProperties.OPTIMIZE_RANGE_SPLITS_ENABLED,
                Boolean.toString(optimizeRangeSplitsEnabled));
        conn.setSessionProperty(
                AccumuloSessionProperties.SECONDARY_INDEX_ENABLED,
                Boolean.toString(secondaryIndexEnabled));

        QueryMetrics qm = new QueryMetrics();
        qm.script = qf.getName();
        qm.optimizeColumnFiltersEnabled = optimizeColumnFiltersEnabled;
        qm.optimizeRangePredicatePushdownEnabled = optimizeRangePredicatePushdownEnabled;
        qm.optimizeRangeSplitsEnabled = optimizeRangeSplitsEnabled;
        qm.secondaryIndexEnabled = secondaryIndexEnabled;

        String query = Files.toString(qf, StandardCharsets.UTF_8);
        LOG.info(format("Executing query %s\n%s", qf.getName(), query));
        Statement stmt = conn.createStatement();
        long start = System.currentTimeMillis();
        ExecutorService ex = Executors.newSingleThreadExecutor();
        Future<?> future = ex.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    PrestoResultSet rs = (PrestoResultSet) stmt
                            .executeQuery(query);
                    qm.queryId = rs.getQueryId();

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();
                    while (rs.next()) {
                        for (int i = 1; i <= columnsNumber; i++) {
                            if (i > 1) {
                                System.out.print("|");
                            }
                            System.out.print(rs.getString(i));
                        }
                        System.out.println();
                    }
                    qm.queryStats = rs.getStats();
                } catch (SQLException e) {
                    e.printStackTrace();
                    qm.error = true;
                }
            }
        });

        try {
            future.get(timeout, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            qm.timedout = true;
            LOG.warn("Query hit timeout threshold, cancelling thread");
        }

        long end = System.currentTimeMillis();
        LOG.info(format("Query %s executed in %d ms", qf.getName(),
                (end - start)));
        qm.queryTimeMS = new Long(end - start);
        conn.close();

        return qm;
    }
}
