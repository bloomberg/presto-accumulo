package bloomberg.presto.accumulo.benchmark;

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.google.common.io.Files;
import org.apache.log4j.Logger;

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

import static java.lang.String.format;

public class TpchQueryExecutor
{

    private static final Logger LOG = Logger.getLogger(TpchQueryExecutor.class);
    private static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String SCHEME = "jdbc:presto://";
    private static final String CATALOG = "accumulo";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static QueryMetrics run(AccumuloConfig accConfig, File qf, String host, int port,
            String schema, boolean optimizeRangeSplitsEnabled, boolean secondaryIndexEnabled, int timeout)
            throws Exception
    {

        String dbUrl = String.format("%s%s:%d/%s/%s", SCHEME, host, port, CATALOG, schema);

        Properties props = new Properties();
        props.setProperty("user", "root");
        PrestoConnection conn = (PrestoConnection) DriverManager.getConnection(dbUrl, props);
        conn.setSessionProperty(AccumuloSessionProperties.OPTIMIZE_RANGE_SPLITS_ENABLED, Boolean.toString(optimizeRangeSplitsEnabled));
        conn.setSessionProperty(AccumuloSessionProperties.OPTIMIZE_INDEX_ENABLED, Boolean.toString(secondaryIndexEnabled));
        conn.setSessionProperty(AccumuloSessionProperties.INDEX_THRESHOLD, Double.toString(.15));
        conn.setSessionProperty(AccumuloSessionProperties.INDEX_LOWEST_CARDINALITY_THRESHOLD, Double.toString(.5));

        QueryMetrics qm = new QueryMetrics();
        qm.script = qf.getName();
        qm.optimizeRangeSplitsEnabled = optimizeRangeSplitsEnabled;
        qm.secondaryIndexEnabled = secondaryIndexEnabled;

        String query = Files.toString(qf, StandardCharsets.UTF_8);
        LOG.info(format("Executing query %s\n%s", qf.getName(), query));
        Statement stmt = conn.createStatement();
        long start = System.currentTimeMillis();
        ExecutorService ex = Executors.newSingleThreadExecutor();
        Future<?> future = ex.submit(new Runnable()
        {

            @Override
            public void run()
            {
                try {
                    PrestoResultSet rs = (PrestoResultSet) stmt.executeQuery(query);
                    qm.queryId = rs.getQueryId();

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();
                    StringBuilder bldr = new StringBuilder();
                    while (rs.next()) {
                        bldr.setLength(0);
                        for (int i = 1; i <= columnsNumber; i++) {
                            if (i > 1) {
                                bldr.append('|');
                            }
                            bldr.append(rs.getString(i));
                        }
                        LOG.info(bldr.toString());
                    }
                    qm.queryStats = rs.getStats();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                    qm.error = true;
                }
            }
        });

        try {
            future.get(timeout, TimeUnit.MINUTES);
        }
        catch (TimeoutException e) {
            future.cancel(true);
            qm.timedout = true;
            LOG.warn("Query hit timeout threshold, cancelling thread");
        }

        long end = System.currentTimeMillis();
        LOG.info(format("Query %s executed in %d ms", qf.getName(), (end - start)));
        qm.queryTimeMS = new Long(end - start);
        conn.close();

        return qm;
    }
}
