package bloomberg.presto.accumulo.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

    private String host = "localhost", schema, query;
    private int port = 8080;
    private List<Row> inputs = new ArrayList<>();
    private List<Row> expectedOutputs = new ArrayList<>();
    private boolean orderMatters = false;
    private String instanceName, zooKeepers, user, password;
    private String tableName;
    private Connector conn;
    private RowSchema inputSchema;

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
        inputs.add(row);
        return this;
    }

    public QueryDriver withOutput(Row row) {
        this.expectedOutputs.add(row);
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

    public QueryDriver orderMatters() {
        orderMatters = true;
        return this;
    }

    public void runTest() throws Exception {
        try {
            createTable();
            pushInput();
            pushPrestoMetadata();
            if (!compareOutput(execQuery())) {
                throw new Exception("Output does not match, see log");
            }
        } finally {
            cleanup();
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

        System.out.println("Connecting to database...");

        Properties props = new Properties();
        props.setProperty("user", "root");
        Connection conn = DriverManager.getConnection(this.getDbUrl(), props);

        System.out.println("Creating statement...");
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(this.getQuery());

        List<Row> outputRows = new ArrayList<>();
        while (rs.next()) {
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
                case Types.VARBINARY:
                    orow.addField(
                            rs.getBlob(j).getBytes(1,
                                    (int) rs.getBlob(j).length()),
                            PrestoType.VARBINARY);
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
        return outputRows;
    }

    /**
     * Compares output of the actual data to the configured expected output
     * 
     * @param actual
     *            The list of results from the query
     * @return True if the output matches, false otherwise
     */
    protected boolean compareOutput(List<Row> actual) {
        if (orderMatters) {
            return validateWithOrder(actual);
        } else {
            return validateWithoutOrder(actual);
        }
    }

    protected void cleanup() throws Exception {

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
    }

    protected boolean validateWithoutOrder(final List<Row> outputs) {
        Set<Integer> verifiedExpecteds = new HashSet<Integer>();
        Set<Integer> unverifiedOutputs = new HashSet<Integer>();
        for (int i = 0; i < outputs.size(); i++) {
            Row output = outputs.get(i);
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

        for (int i = 0; i < outputs.size(); i++) {
            if (unverifiedOutputs.contains(i)) {
                LOG.error("Received unexpected output %s", outputs.get(i));
                noErrors = false;
            }
        }

        return noErrors;
    }

    protected boolean validateWithOrder(final List<Row> outputs) {
        boolean noErrors = true;
        int i = 0;
        for (i = 0; i < Math.min(outputs.size(), expectedOutputs.size()); i++) {
            Row output = outputs.get(i);
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

        for (int j = i; j < outputs.size(); j++) {
            LOG.error("Received unexpected output %s at position %d.",
                    outputs.get(j), j);
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
