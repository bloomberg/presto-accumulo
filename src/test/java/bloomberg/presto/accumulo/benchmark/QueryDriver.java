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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import bloomberg.presto.accumulo.AccumuloColumnMetadataProvider;
import bloomberg.presto.accumulo.PrestoType;
import bloomberg.presto.accumulo.metadata.ZooKeeperMetadataCreator;
import io.airlift.log.Logger;

public class QueryDriver {
    private static final Logger LOG = Logger.get(QueryDriver.class);
    public static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    public static final String SCHEME = "jdbc:presto://";
    public static final String CATALOG = "accumulo";
    public static final String ZK_METADATA_ROOT = "/presto-accumulo";

    private boolean initialized = false, orderMatters = false;
    private int port = 8080;
    private String host = "localhost", schema, query, instanceName, zooKeepers,
            user, password, tableName;
    private Connector conn;
    private List<Row> inputs = new ArrayList<>(),
            expectedOutputs = new ArrayList<>();
    private RowSchema inputSchema, outputSchema;

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public QueryDriver(String instanceName, String zookeepers, String user,
            String password)
                    throws AccumuloException, AccumuloSecurityException {
        this.instanceName = instanceName;
        this.zooKeepers = zookeepers;
        this.user = user;
        this.password = password;
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
        this.inputs.addAll(rows);
        return this;
    }

    public QueryDriver withOutput(Row row) {
        this.expectedOutputs.add(row);
        return this;
    }

    public QueryDriver withOutput(List<Row> rows) {
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
        if (this.inputSchema == null) {
            throw new RuntimeException(
                    "Input schema must be set prior to using this method");
        }

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
                    switch (rSchema.getColumn(i).getType()) {
                    case BIGINT:
                        r.addField(Long.parseLong(tokens[i]),
                                PrestoType.BIGINT);
                        break;
                    case BOOLEAN:
                        r.addField(Boolean.parseBoolean(tokens[i]),
                                PrestoType.BOOLEAN);
                        break;
                    case DATE:
                        r.addField(new Date(Long.parseLong(tokens[i])),
                                PrestoType.DATE);
                        break;
                    case DOUBLE:
                        r.addField(Double.parseDouble(tokens[i]),
                                PrestoType.DOUBLE);
                        break;
                    case TIME:
                        r.addField(new Time(Long.parseLong(tokens[i])),
                                PrestoType.TIME);
                        break;
                    case TIMESTAMP:
                        r.addField(new Timestamp(Long.parseLong(tokens[i])),
                                PrestoType.TIMESTAMP);
                        break;
                    case VARBINARY:
                        r.addField(tokens[i].getBytes(), PrestoType.VARBINARY);
                        break;
                    case VARCHAR:
                        r.addField(tokens[i], PrestoType.VARCHAR);
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
        if (!initialized) {
            initialize();
        }
        return execQuery();
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
        CuratorFramework zkclient = CuratorFrameworkFactory
                .newClient(zooKeepers, new ExponentialBackoffRetry(1000, 3));
        zkclient.start();

        String schemaPath = String.format("%s/%s/%s", ZK_METADATA_ROOT, schema,
                tableName);
        String tablePath = String.format("%s/%s/%s", ZK_METADATA_ROOT, schema,
                tableName);

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
        ZooKeeperInstance inst = new ZooKeeperInstance(instanceName,
                zooKeepers);
        this.conn = inst.getConnector(user, new PasswordToken(password));
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
        BatchWriterConfig conf = new BatchWriterConfig();
        conf.setMaxLatency(120, TimeUnit.SECONDS);
        conf.setMaxMemory(50 * 1024 * 1024);
        conf.setMaxWriteThreads(10);
        BatchWriter wrtr = conn.createBatchWriter(tableName, conf);

        // get the row id index from the schema
        int rowIdIdx = inputSchema
                .getColumn(AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME)
                .getOrdinal();

        for (Row row : inputs) {
            int i = 0;
            // make a new mutation, passing in the row ID
            Mutation m = new Mutation(
                    row.getField(rowIdIdx).getValue().toString());
            // for each column in the input schema
            for (Column c : inputSchema.getColumns()) {
                // if this column's name is not the row ID
                if (!c.getPrestoName().equals(
                        AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME)) {
                    switch (c.getType()) {
                    case DATE:
                        m.put(c.getColumnFamily(), c.getColumnQualifier(),
                                Long.toString(TimeUnit.MILLISECONDS.toDays(
                                        row.getField(i).getDate().getTime())));
                        break;
                    case TIME:
                        m.put(c.getColumnFamily(), c.getColumnQualifier(), Long
                                .toString(row.getField(i).getTime().getTime()));
                        break;
                    case TIMESTAMP:
                        m.put(c.getColumnFamily(), c.getColumnQualifier(),
                                Long.toString(row.getField(i).getTimestamp()
                                        .getTime()));
                        break;
                    case VARBINARY:
                        m.put(c.getColumnFamily(), c.getColumnQualifier(),
                                new Value(row.getField(i).getVarBinary()));
                        break;
                    default:
                        m.put(c.getColumnFamily(), c.getColumnQualifier(),
                                row.getField(i).getValue().toString());
                        break;
                    }
                }
                ++i;
            }

            wrtr.addMutation(m);
        }

        wrtr.close();
    }

    protected void pushPrestoMetadata() throws Exception {
        ZooKeeperMetadataCreator creator = new ZooKeeperMetadataCreator();
        creator.setZooKeepers(zooKeepers);
        creator.setNamespace(getSchema());
        creator.setTable(getAccumuloTable());
        creator.setMetadataRoot(ZK_METADATA_ROOT);
        creator.setForce(true);

        for (Column c : inputSchema.getColumns()) {
            if (!c.getPrestoName().equals(
                    AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME)) {
                creator.setColumnFamily(c.getColumnFamily());
                creator.setColumnQualifier(c.getColumnQualifier());
                creator.setPrestoColumn(c.getPrestoName());
                creator.setPrestoType(c.getType().toString());
                creator.createMetadata();
            }
        }
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
                    orow.addField(rs.getLong(j), PrestoType.BIGINT);
                    break;
                case Types.BOOLEAN:
                    orow.addField(rs.getBoolean(j), PrestoType.BOOLEAN);
                    break;
                case Types.DATE:
                    orow.addField(rs.getDate(j), PrestoType.DATE);
                    break;
                case Types.DOUBLE:
                    orow.addField(rs.getDouble(j), PrestoType.DOUBLE);
                    break;
                case Types.TIME:
                    orow.addField(rs.getTime(j), PrestoType.TIME);
                    break;
                case Types.TIMESTAMP:
                    orow.addField(rs.getTimestamp(j), PrestoType.TIMESTAMP);
                    break;
                case Types.LONGVARBINARY:
                    orow.addField(rs.getBytes(j), PrestoType.VARBINARY);
                    break;
                case Types.LONGNVARCHAR:
                    orow.addField(rs.getString(j), PrestoType.VARCHAR);
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
