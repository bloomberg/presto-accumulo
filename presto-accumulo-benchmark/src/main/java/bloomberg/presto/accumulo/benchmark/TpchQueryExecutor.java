package bloomberg.presto.accumulo.benchmark;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.io.Files;

import bloomberg.presto.accumulo.AccumuloConfig;
import io.airlift.log.Logger;

public class TpchQueryExecutor {

    private static final Logger LOG = Logger.get(TpchQueryExecutor.class);
    public static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    public static final String SCHEME = "jdbc:presto://";
    public static final String CATALOG = "accumulo";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void run(AccumuloConfig accConfig, String host, int port,
            String schema, File scriptsDir) throws Exception {

        String dbUrl = String.format("%s%s:%d/%s/%s", SCHEME, host, port,
                CATALOG, schema);

        Properties props = new Properties();
        props.setProperty("user", "root");
        Connection conn = DriverManager.getConnection(dbUrl, props);

        List<File> queryFiles = Arrays.asList(scriptsDir.listFiles()).stream()
                .filter(x -> x.getName().matches("[0-9]+.sql"))
                .collect(Collectors.toList());

        for (File qf : queryFiles) {
            Statement stmt = conn.createStatement();
            long start = System.currentTimeMillis();
            stmt.execute(Files.toString(qf, StandardCharsets.UTF_8));
            long end = System.currentTimeMillis();
            LOG.info("Query %s executed in %d ms", qf.getName(), (end - start));
        }
    }
}
