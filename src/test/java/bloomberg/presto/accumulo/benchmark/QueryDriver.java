package bloomberg.presto.accumulo.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloPageSink;
import bloomberg.presto.accumulo.AccumuloTable;
import bloomberg.presto.accumulo.metadata.ZooKeeperMetadataManager;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import io.airlift.log.Logger;

public class QueryDriver {
    private static final Logger LOG = Logger.get(QueryDriver.class);
    public static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    public static final String SCHEME = "jdbc:presto://";
    public static final String CATALOG = "accumulo";

    private boolean initialized = false, orderMatters = false;
    private int port = 8080;
    private final AccumuloConfig config;
    private String host = "localhost", schema, query, tableName;
    private Connector conn;
    private List<Row> inputs = new ArrayList<>(),
            expectedOutputs = new ArrayList<>();
    private RowSchema inputSchema, outputSchema;
    private AccumuloRowSerializer serializer;
    private ZooKeeperMetadataManager metaManager;

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public QueryDriver(AccumuloConfig config)
            throws AccumuloException, AccumuloSecurityException {
        this(config, AccumuloRowSerializer.getDefault());
    }

    public QueryDriver(AccumuloConfig config, AccumuloRowSerializer serializer)
            throws AccumuloException, AccumuloSecurityException {
        this.config = config;
        this.serializer = serializer;
        initAccumulo();
    }

    public QueryDriver withHost(String host) {
        this.host = host;
        return this;
    }

    public QueryDriver withPort(int port) {
        this.port = port;
        return this;
    }

    public QueryDriver withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public QueryDriver withQuery(String query) {
        this.query = query;
        return this;
    }

    public QueryDriver withInput(Row row) {
        this.inputs.add(row);
        return this;
    }

    public QueryDriver withInput(List<Row> rows) {
        this.inputs.clear();
        this.inputs.addAll(rows);
        return this;
    }

    public QueryDriver withOutput(Row row) {
        this.expectedOutputs.add(row);
        return this;
    }

    public QueryDriver withOutput(List<Row> rows) {
        this.expectedOutputs.clear();
        this.expectedOutputs.addAll(rows);
        return this;
    }

    public QueryDriver withTable(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public QueryDriver withInputSchema(RowSchema schema) {
        this.inputSchema = schema;
        return this;
    }

    public QueryDriver withInputFile(File file) throws IOException {
        if (this.inputSchema == null) {
            throw new RuntimeException(
                    "Input schema must be set prior to using this method");
        }

        this.withInput(loadRowsFromFile(inputSchema, file));

        return this;
    }

    public QueryDriver withOutputSchema(RowSchema schema) {
        this.outputSchema = schema;
        return this;
    }

    public QueryDriver withOutputFile(File file) throws IOException {
        if (this.outputSchema == null) {
            throw new RuntimeException(
                    "Input schema must be set prior to using this method");
        }

        this.expectedOutputs.clear();
        this.withOutput(loadRowsFromFile(outputSchema, file));

        return this;
    }

    public static List<Row> loadRowsFromFile(RowSchema rSchema, File file)
            throws IOException {
        List<Row> list = new ArrayList<>();
        int eLength = rSchema.getLength();

        // auto-detect gzip compression based on filename
        InputStream dataStream = file.getName().endsWith(".gz")
                ? new GZIPInputStream(new FileInputStream(file))
                : new FileInputStream(file);

        try (BufferedReader rdr = new BufferedReader(
                new InputStreamReader(dataStream))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                String[] tokens = line.split("\\|");

                if (tokens.length != eLength) {
                    throw new RuntimeException(String.format(
                            "Record in file has %d tokens, expected %d, %s",
                            tokens.length, eLength, line));
                }

                Row r = Row.newInstance();
                list.add(r);
                for (int i = 0; i < tokens.length; ++i) {
                    switch (rSchema.getColumn(i).getType().getDisplayName()) {
                    case StandardTypes.BIGINT:
                        r.addField(Long.parseLong(tokens[i]),
                                BigintType.BIGINT);
                        break;
                    case StandardTypes.BOOLEAN:
                        r.addField(Boolean.parseBoolean(tokens[i]),
                                BooleanType.BOOLEAN);
                        break;
                    case StandardTypes.DATE:
                        r.addField(new Date(Long.parseLong(tokens[i])),
                                DateType.DATE);
                        break;
                    case StandardTypes.DOUBLE:
                        r.addField(Double.parseDouble(tokens[i]),
                                DoubleType.DOUBLE);
                        break;
                    case StandardTypes.TIME:
                        r.addField(new Time(Long.parseLong(tokens[i])),
                                TimeType.TIME);
                        break;
                    case StandardTypes.TIMESTAMP:
                        r.addField(new Timestamp(Long.parseLong(tokens[i])),
                                TimestampType.TIMESTAMP);
                        break;
                    case StandardTypes.VARBINARY:
                        r.addField(tokens[i].getBytes(),
                                VarbinaryType.VARBINARY);
                        break;
                    case StandardTypes.VARCHAR:
                        r.addField(tokens[i], VarcharType.VARCHAR);
                        break;
                    default:
                        break;
                    }
                }
            }
        }
        return list;
    }

    public QueryDriver orderMatters() {
        orderMatters = true;
        return this;
    }

    public List<Row> run() throws Exception {
        try {
            if (!initialized) {
                initialize();
            }
            return execQuery();
        } finally {
            inputs.clear();
            expectedOutputs.clear();
        }
    }

    public void runTest() throws Exception {
        List<Row> eOutputs = new ArrayList<Row>(expectedOutputs);
        if (!compareOutput(run(), eOutputs, orderMatters)) {
            throw new Exception("Output does not match, see log");
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getSchema() {
        return schema;
    }

    public String getQuery() {
        return query;
    }

    public String getAccumuloTable() {
        return tableName;
    }

    public void initialize() throws Exception {
        createTable();
        pushInput();
        pushPrestoMetadata();
        initialized = true;
    }

    /**
     * Cleans up all state regarding this QueryDriver as if it had just been
     * created.
     * 
     * Deletes the Accumulo table, deletes the metadata in ZooKeeper, clears
     * configured inputs, outputs, if order matters, the table name, and input
     * schema.
     * 
     * @throws Exception
     *             Bad juju
     */
    public void cleanup() throws Exception {

        // delete the accumulo table
        if (this.getSchema().equals("default")) {
            conn.tableOperations().delete(this.getAccumuloTable());
        } else {
            conn.tableOperations()
                    .delete(this.getSchema() + "." + this.getAccumuloTable());
        }

        // cleanup metadata folder
        CuratorFramework zkclient = CuratorFrameworkFactory.newClient(
                config.getZooKeepers(), new ExponentialBackoffRetry(1000, 3));
        zkclient.start();

        String schemaPath = String.format("%s/%s/%s",
                config.getZkMetadataRoot(), schema, tableName);
        String tablePath = String.format("%s/%s/%s", config.getZkMetadataRoot(),
                schema, tableName);

        if (zkclient.checkExists().forPath(tablePath) != null) {
            zkclient.delete().deletingChildrenIfNeeded().forPath(tablePath);
        }

        if (zkclient.checkExists().forPath(schemaPath) != null
                && zkclient.getChildren().forPath(schemaPath).isEmpty()) {
            zkclient.delete().deletingChildrenIfNeeded().forPath(schemaPath);
        }

        this.withHost("localhost").withPort(8080).withInputSchema(null)
                .withSchema(null).withQuery(null).withTable(null);

        inputs.clear();
        expectedOutputs.clear();
        orderMatters = false;
        initialized = false;
    }

    /**
     * Compares output of the actual data to the configured expected output
     * 
     * @param actual
     *            The list of results from the query
     * @param expected
     *            The list of expected results
     * @return True if the output matches, false otherwise
     */
    public static boolean compareOutput(List<Row> actual, List<Row> expected,
            boolean orderMatters) {
        if (orderMatters) {
            return validateWithOrder(actual, expected);
        } else {
            return validateWithoutOrder(actual, expected);
        }
    }

    protected String getDbUrl() {
        return String.format("%s%s:%d/%s/%s", SCHEME, getHost(), getPort(),
                CATALOG, getSchema());
    }

    protected void initAccumulo()
            throws AccumuloException, AccumuloSecurityException {
        ZooKeeperInstance inst = new ZooKeeperInstance(config.getInstance(),
                config.getZooKeepers());
        this.conn = inst.getConnector(config.getUsername(),
                new PasswordToken(config.getPassword()));
        metaManager = new ZooKeeperMetadataManager(CATALOG, config);
    }

    protected void createTable() throws AccumuloException,
            AccumuloSecurityException, TableExistsException {
        if (this.getSchema().equals("default")) {
            conn.tableOperations().create(this.getAccumuloTable());
        } else {
            conn.tableOperations()
                    .create(this.getSchema() + "." + this.getAccumuloTable());
        }
    }

    protected void pushInput()
            throws TableNotFoundException, MutationsRejectedException {
        BatchWriter wrtr = conn.createBatchWriter(tableName,
                new BatchWriterConfig());

        for (Row row : inputs) {
            // So... the deal here is we are converting the Date objects,
            // specified by the user, to the number of days because that is what
            // Presto wants

            // Chose to just do this here instead of putting the burden on the
            // programmer using this library for converting the Date types to #
            // of days

            // Maybe I will regret this later, but for now, check out this cool
            // stream stuff
            Row toMutate = new Row(row);
            toMutate.getFields().stream()
                    .filter(x -> x.getType().equals(DateType.DATE))
                    .forEach(x -> x.setDate(TimeUnit.MILLISECONDS
                            .toDays(x.getDate().getTime())));
            wrtr.addMutation(AccumuloPageSink.toMutation(toMutate,
                    inputSchema.getColumns(), serializer));
        }

        wrtr.close();
    }

    protected void pushPrestoMetadata() throws Exception {
        metaManager.createTableMetadata(new AccumuloTable(getSchema(),
                getAccumuloTable(), inputSchema.getColumns(),
                this.serializer.getClass().getName()));
    }

    protected List<Row> execQuery() throws SQLException {

        LOG.info("Connecting to database...");

        Properties props = new Properties();
        props.setProperty("user", "root");
        Connection conn = DriverManager.getConnection(this.getDbUrl(), props);

        LOG.info("Creating statement...");
        Statement stmt = conn.createStatement();

        LOG.info("Executing query...");
        long start = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery(this.getQuery());

        long numrows = 0;
        List<Row> outputRows = new ArrayList<>();
        while (rs.next()) {
            ++numrows;
            Row orow = new Row();
            outputRows.add(orow);
            for (int j = 1; j <= rs.getMetaData().getColumnCount(); ++j) {
                switch (rs.getMetaData().getColumnType(j)) {
                case Types.BIGINT:
                    orow.addField(rs.getLong(j), BigintType.BIGINT);
                    break;
                case Types.BOOLEAN:
                    orow.addField(rs.getBoolean(j), BooleanType.BOOLEAN);
                    break;
                case Types.DATE:
                    orow.addField(rs.getDate(j), DateType.DATE);
                    break;
                case Types.DOUBLE:
                    orow.addField(rs.getDouble(j), DoubleType.DOUBLE);
                    break;
                case Types.TIME:
                    orow.addField(rs.getTime(j), TimeType.TIME);
                    break;
                case Types.TIMESTAMP:
                    orow.addField(rs.getTimestamp(j), TimestampType.TIMESTAMP);
                    break;
                case Types.LONGVARBINARY:
                    orow.addField(rs.getBytes(j), VarbinaryType.VARBINARY);
                    break;
                case Types.LONGNVARCHAR:
                    orow.addField(rs.getString(j), VarcharType.VARCHAR);
                    break;
                default:
                    throw new RuntimeException("Unknown SQL type "
                            + rs.getMetaData().getColumnType(j));
                }
            }
        }

        rs.close();
        stmt.close();
        conn.close();

        long end = System.currentTimeMillis();
        LOG.info(String.format("Done.  Received %d rows in %d ms", numrows,
                end - start));
        return outputRows;
    }

    protected static boolean validateWithoutOrder(final List<Row> actualOutputs,
            final List<Row> expectedOutputs) {
        Set<Integer> verifiedExpecteds = new HashSet<Integer>();
        Set<Integer> unverifiedOutputs = new HashSet<Integer>();
        for (int i = 0; i < actualOutputs.size(); i++) {
            Row output = actualOutputs.get(i);
            boolean found = false;
            for (int j = 0; j < expectedOutputs.size(); j++) {
                if (verifiedExpecteds.contains(j)) {
                    continue;
                }
                Row expected = expectedOutputs.get(j);
                if (expected.equals(output)) {
                    found = true;
                    verifiedExpecteds.add(j);
                    LOG.info(
                            String.format("Matched expected output %s no %d at "
                                    + "position %d", output, j, i));
                    break;
                }
            }
            if (!found) {
                unverifiedOutputs.add(i);
            }
        }

        boolean noErrors = true;
        for (int j = 0; j < expectedOutputs.size(); j++) {
            if (!verifiedExpecteds.contains(j)) {
                LOG.error("Missing expected output %s", expectedOutputs.get(j));
                noErrors = false;
            }
        }

        for (int i = 0; i < actualOutputs.size(); i++) {
            if (unverifiedOutputs.contains(i)) {
                LOG.error("Received unexpected output %s",
                        actualOutputs.get(i));
                noErrors = false;
            }
        }

        return noErrors;
    }

    protected static boolean validateWithOrder(final List<Row> actualOutputs,
            final List<Row> expectedOutputs) {
        boolean noErrors = true;
        int i = 0;
        for (i = 0; i < Math.min(actualOutputs.size(),
                expectedOutputs.size()); i++) {
            Row output = actualOutputs.get(i);
            Row expected = expectedOutputs.get(i);
            if (expected.equals(output)) {
                LOG.info(String.format(
                        "Matched expected output %s at " + "position %d",
                        expected, i));
            } else {
                LOG.error("Missing expected output %s at position %d, got %s.",
                        expected, i, output);
                noErrors = false;
            }
        }

        for (int j = i; j < actualOutputs.size(); j++) {
            LOG.error("Received unexpected output %s at position %d.",
                    actualOutputs.get(j), j);
            noErrors = false;
        }

        for (int j = i; j < expectedOutputs.size(); j++) {
            LOG.error("Missing expected output %s at position %d.",
                    expectedOutputs.get(j), j);
            noErrors = false;
        }
        return noErrors;
    }
}
