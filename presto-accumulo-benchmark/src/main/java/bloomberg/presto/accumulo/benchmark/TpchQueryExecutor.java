package bloomberg.presto.accumulo.benchmark;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import bloomberg.presto.accumulo.AccumuloConfig;
import io.airlift.log.Logger;

public class TpchQueryExecutor {

    private static final Logger LOG = Logger.get(TpchQueryExecutor.class);
    private static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String SCHEME = "jdbc:presto://";
    private static final String CATALOG = "accumulo";
    private static List<String> BLACKLIST = ImmutableList
            .copyOf(new String[] { "2.sql", "4.sql", "11.sql", "13.sql",
                    "15.sql", "17.sql", "20.sql", "21.sql", "22.sql" });

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<QueryMetrics> run(AccumuloConfig accConfig, String host,
            int port, String schema, File scriptsDir) throws Exception {

        List<QueryMetrics> metrics = new ArrayList<>();

        String dbUrl = String.format("%s%s:%d/%s/%s", SCHEME, host, port,
                CATALOG, schema);

        Properties props = new Properties();
        props.setProperty("user", "root");
        Connection conn = DriverManager.getConnection(dbUrl, props);

        List<File> queryFiles = Arrays.asList(scriptsDir.listFiles()).stream()
                .filter(x -> !BLACKLIST.contains(x.getName())
                        && x.getName().matches("[0-9]+.sql"))
                .collect(Collectors.toList());

        for (File qf : queryFiles) {
            QueryMetrics qm = new QueryMetrics();
            qm.script = qf.getName();
            try {
                String query = Files.toString(qf, StandardCharsets.UTF_8);
                LOG.info("Executing query %s\n%s", qf.getName(), query);
                Statement stmt = conn.createStatement();
                long start = System.currentTimeMillis();
                stmt.execute(query);
                long end = System.currentTimeMillis();
                LOG.info("Query %s executed in %d ms", qf.getName(),
                        (end - start));
                qm.queryTimeMS = new Long(end - start);
            } catch (SQLException e) {
                e.printStackTrace();
                qm.error = true;
            }
            metrics.add(qm);
        }
        return metrics;
    }
}
