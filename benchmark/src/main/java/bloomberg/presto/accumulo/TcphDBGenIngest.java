package bloomberg.presto.accumulo;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;

public class TcphDBGenIngest extends Configured implements Tool {

    private static final char DELIMITER = '|';
    private static final String CUSTOMER_ROW_ID = "custkey";
    private static final String LINEITEM_ROW_ID = "orderkey";
    private static final String NATION_ROW_ID = "nationkey";
    private static final String ORDERS_ROW_ID = "orderkey";
    private static final String PART_ROW_ID = "partkey";
    private static final String PARTSUPP_ROW_ID = "partkey";
    private static final String REGION_ROW_ID = "regionkey";
    private static final String SUPPLIER_ROW_ID = "suppkey";

    private static final RowSchema CUSTOMER_SCHEMA = RowSchema.newInstance()
            .addColumn("custkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("address", "md", "address", VARCHAR)
            .addColumn("nationkey", "md", "nationkey", BIGINT)
            .addColumn("phone", "md", "phone", VARCHAR)
            .addColumn("acctbal", "md", "acctbal", DOUBLE)
            .addColumn("mktsegment", "md", "mktsegment", VARCHAR)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema LINEITEM_SCHEMA = RowSchema.newInstance()
            .addColumn("orderkey", null, null, BIGINT)
            .addColumn("partkey", "md", "partkey", BIGINT)
            .addColumn("suppkey", "md", "suppkey", BIGINT)
            .addColumn("linenumber", "md", "linenumber", BIGINT)
            .addColumn("quantity", "md", "quantity", BIGINT)
            .addColumn("extendedprice", "md", "extendedprice", DOUBLE)
            .addColumn("discount", "md", "discount", DOUBLE)
            .addColumn("tax", "md", "tax", DOUBLE)
            .addColumn("returnflag", "md", "returnflag", VARCHAR)
            .addColumn("linestatus", "md", "linestatus", VARCHAR)
            .addColumn("shipdate", "md", "shipdate", DATE)
            .addColumn("commitdate", "md", "commitdate", DATE)
            .addColumn("receiptdate", "md", "receiptdate", DATE)
            .addColumn("shipinstruct", "md", "shipinstruct", VARCHAR)
            .addColumn("shipmode", "md", "shipmode", VARCHAR)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema NATION_SCHEMA = RowSchema.newInstance()
            .addColumn("nationkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("regionkey", "md", "regionkey", BIGINT)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema ORDERS_SCHEMA = RowSchema.newInstance()
            .addColumn("orderkey", null, null, BIGINT)
            .addColumn("custkey", "md", "custkey", BIGINT)
            .addColumn("orderstatus", "md", "orderstatus", VARCHAR)
            .addColumn("totalprice", "md", "totalprice", DOUBLE)
            .addColumn("orderdate", "md", "orderdate", DATE)
            .addColumn("orderpriority", "md", "orderpriority", VARCHAR)
            .addColumn("clerk", "md", "clerk", VARCHAR)
            .addColumn("shippriority", "md", "shippriority", BIGINT)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema PART_SCHEMA = RowSchema.newInstance()
            .addColumn("partkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("mfgr", "md", "mfgr", VARCHAR)
            .addColumn("brand", "md", "brand", VARCHAR)
            .addColumn("type", "md", "type", VARCHAR)
            .addColumn("size", "md", "size", BIGINT)
            .addColumn("container", "md", "container", VARCHAR)
            .addColumn("retailprice", "md", "retailprice", DOUBLE)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema PARTSUPP_SCHEMA = RowSchema.newInstance()
            .addColumn("partkey", null, null, BIGINT)
            .addColumn("suppkey", "md", "suppkey", BIGINT)
            .addColumn("availqty", "md", "availqty", BIGINT)
            .addColumn("supplycost", "md", "supplycost", DOUBLE)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema REGION_SCHEMA = RowSchema.newInstance()
            .addColumn("regionkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema SUPPLIER_SCHEMA = RowSchema.newInstance()
            .addColumn("suppkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("address", "md", "address", VARCHAR)
            .addColumn("nationkey", "md", "nationkey", BIGINT)
            .addColumn("phone", "md", "phone", VARCHAR)
            .addColumn("acctbal", "md", "acctbal", DOUBLE)
            .addColumn("comment", "md", "comment", VARCHAR);

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TcphDBGenIngest(),
                args));
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 6) {
            System.err
                    .println("Usage: [instance] [zookeepers] [user] [passwd] [presto.schema] [tpch.dir]");

            return 1;
        }

        String instance = args[0];
        String zookeepers = args[1];
        String user = args[2];
        String passwd = args[3];
        String schema = args[4];
        File tpchDir = new File(args[5]);

        if (!tpchDir.exists()) {
            throw new FileNotFoundException(
                    "Given datagen directory does not exist");
        }

        List<File> dataFiles = Arrays.asList(tpchDir.listFiles()).stream()
                .filter(x -> x.getName().endsWith(".tbl"))
                .collect(Collectors.toList());

        if (dataFiles.isEmpty()) {
            throw new FileNotFoundException(
                    "No table files found in datagen directory");
        }

        AccumuloConfig accConfig = new AccumuloConfig();
        accConfig.setInstance(instance);
        accConfig.setPassword(passwd);
        accConfig.setUsername(user);
        accConfig.setZooKeepers(zookeepers);

        AccumuloMetadataManager mgr = AccumuloMetadataManager.getDefault(
                "accumulo", accConfig);

        ZooKeeperInstance inst = new ZooKeeperInstance(instance, zookeepers);
        Connector conn = inst.getConnector(user, new PasswordToken(passwd));
        AccumuloRowSerializer serializer = AccumuloRowSerializer.getDefault();
        for (File df : dataFiles) {
            String tableName = FilenameUtils.removeExtension(df.getName());
            String fullTableName = schema + '.' + tableName;

            RowSchema rowSchema = schemaFromFile(tableName);
            String rowIdName = rowIdFromFile(tableName);

            AccumuloTable table = new AccumuloTable(schema, tableName,
                    rowSchema.getColumns(), rowIdName, true, serializer
                            .getClass().getCanonicalName());

            mgr.createTableMetadata(table);

            if (!conn.namespaceOperations().exists(schema)) {
                conn.namespaceOperations().create(schema);
            }

            conn.tableOperations().create(fullTableName);

            System.out.println(String.format("Created table %s", table));
            System.out.println(String.format(
                    "Reading rows from file %s, writing to table %s", df,
                    fullTableName));

            BufferedReader rdr = new BufferedReader(new FileReader(df));
            BatchWriterConfig bwc = new BatchWriterConfig();
            BatchWriter wrtr = conn.createBatchWriter(fullTableName, bwc);
            String line;
            int numRows = 0;
            while ((line = rdr.readLine()) != null) {

                Row r = Row.fromString(rowSchema, line, DELIMITER);

                Mutation m = AccumuloPageSink.toMutation(r, rowIdName,
                        rowSchema.getColumns(), serializer);

                wrtr.addMutation(m);
                ++numRows;
            }

            wrtr.flush();
            wrtr.close();

            rdr.close();
            System.out.println(String.format("Wrote %d rows", numRows));
        }

        return 0;
    }

    private String rowIdFromFile(String tableName) {
        switch (tableName) {
        case "customer":
            return CUSTOMER_ROW_ID;
        case "lineitem":
            return LINEITEM_ROW_ID;
        case "nation":
            return NATION_ROW_ID;
        case "orders":
            return ORDERS_ROW_ID;
        case "part":
            return PART_ROW_ID;
        case "partsupp":
            return PARTSUPP_ROW_ID;
        case "region":
            return REGION_ROW_ID;
        case "supplier":
            return SUPPLIER_ROW_ID;
        default:
            throw new InvalidParameterException("Unknown row ID for table "
                    + tableName);
        }
    }

    private RowSchema schemaFromFile(String tableName) {
        switch (tableName) {
        case "customer":
            return CUSTOMER_SCHEMA;
        case "lineitem":
            return LINEITEM_SCHEMA;
        case "nation":
            return NATION_SCHEMA;
        case "orders":
            return ORDERS_SCHEMA;
        case "part":
            return PART_SCHEMA;
        case "partsupp":
            return PARTSUPP_SCHEMA;
        case "region":
            return REGION_SCHEMA;
        case "supplier":
            return SUPPLIER_SCHEMA;
        default:
            throw new InvalidParameterException("Unknown schema for table "
                    + tableName);
        }
    }
}
