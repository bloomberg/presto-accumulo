package bloomberg.presto.accumulo;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.io.FilenameUtils;

import bloomberg.presto.accumulo.index.Utils;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;

public class TpchDBGenIngest {

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
            .addColumn("mktsegment", "md", "mktsegment", VARCHAR, true)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema LINEITEM_SCHEMA = RowSchema.newInstance()
            .addColumn("orderkey", null, null, BIGINT)
            .addColumn("partkey", "md", "partkey", BIGINT)
            .addColumn("suppkey", "md", "suppkey", BIGINT)
            .addColumn("linenumber", "md", "linenumber", BIGINT)
            .addColumn("quantity", "md", "quantity", BIGINT, true)
            .addColumn("extendedprice", "md", "extendedprice", DOUBLE)
            .addColumn("discount", "md", "discount", DOUBLE, true)
            .addColumn("tax", "md", "tax", DOUBLE)
            .addColumn("returnflag", "md", "returnflag", VARCHAR, true)
            .addColumn("linestatus", "md", "linestatus", VARCHAR)
            .addColumn("shipdate", "md", "shipdate", DATE, true)
            .addColumn("commitdate", "md", "commitdate", DATE)
            .addColumn("receiptdate", "md", "receiptdate", DATE, true)
            .addColumn("shipinstruct", "md", "shipinstruct", VARCHAR, true)
            .addColumn("shipmode", "md", "shipmode", VARCHAR, true)
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
            .addColumn("orderdate", "md", "orderdate", DATE, true)
            .addColumn("orderpriority", "md", "orderpriority", VARCHAR)
            .addColumn("clerk", "md", "clerk", VARCHAR)
            .addColumn("shippriority", "md", "shippriority", BIGINT)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema PART_SCHEMA = RowSchema.newInstance()
            .addColumn("partkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("mfgr", "md", "mfgr", VARCHAR)
            .addColumn("brand", "md", "brand", VARCHAR, true)
            .addColumn("type", "md", "type", VARCHAR, true)
            .addColumn("size", "md", "size", BIGINT, true)
            .addColumn("container", "md", "container", VARCHAR, true)
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

    public static void run(AccumuloConfig accConfig, String schema,
            File dbgenDir) throws Exception {

        if (!dbgenDir.exists()) {
            throw new FileNotFoundException(
                    "Given datagen directory does not exist");
        }

        List<File> dataFiles = Arrays.asList(dbgenDir.listFiles()).stream()
                .filter(x -> x.getName().endsWith(".tbl"))
                .collect(Collectors.toList());

        if (dataFiles.isEmpty()) {
            throw new FileNotFoundException(
                    "No table files found in datagen directory");
        }

        AccumuloMetadataManager mgr = AccumuloMetadataManager
                .getDefault("accumulo", accConfig);

        ZooKeeperInstance inst = new ZooKeeperInstance(accConfig.getInstance(),
                accConfig.getZooKeepers());
        Connector conn = inst.getConnector(accConfig.getUsername(),
                new PasswordToken(accConfig.getPassword()));
        AccumuloRowSerializer serializer = AccumuloRowSerializer.getDefault();
        for (File df : dataFiles) {
            String tableName = FilenameUtils.removeExtension(df.getName());
            String fullTableName = schema + '.' + tableName;
            String indexTableName = fullTableName + "_idx";

            RowSchema rowSchema = schemaFromFile(tableName);
            String rowIdName = rowIdFromFile(tableName);
            Map<ByteBuffer, Set<ByteBuffer>> indexColumns = indexColumnsFromFile(
                    tableName);

            AccumuloTable table = new AccumuloTable(schema, tableName,
                    rowSchema.getColumns(), rowIdName, true,
                    serializer.getClass().getCanonicalName());

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

            BatchWriter idxWrtr = null;
            if (indexColumns.size() > 0) {
                conn.tableOperations().create(indexTableName);
                idxWrtr = conn.createBatchWriter(indexTableName, bwc);
            }

            String line;
            int numRows = 0, numIdxRows = 0;
            Collection<Mutation> updates = new HashSet<>();
            while ((line = rdr.readLine()) != null) {

                Row r = Row.fromString(rowSchema, line, DELIMITER);

                Mutation m = AccumuloPageSink.toMutation(r, rowIdName,
                        rowSchema.getColumns(), serializer);

                wrtr.addMutation(m);

                if (idxWrtr != null) {
                    Utils.indexMutation(m, indexColumns, updates);
                    idxWrtr.addMutations(updates);
                    numIdxRows += updates.size();
                    updates.clear();
                }

                ++numRows;
            }

            wrtr.flush();
            wrtr.close();

            rdr.close();
            System.out.println(String.format("Wrote %d rows, %d index rows",
                    numRows, numIdxRows));
        }
    }

    private static Map<ByteBuffer, Set<ByteBuffer>> indexColumnsFromFile(
            String tableName) {
        return Utils
                .getMapOfIndexedColumns(schemaFromFile(tableName).getColumns());
    }

    private static String rowIdFromFile(String tableName) {
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
            throw new InvalidParameterException(
                    "Unknown row ID for table " + tableName);
        }
    }

    private static RowSchema schemaFromFile(String tableName) {
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
            throw new InvalidParameterException(
                    "Unknown schema for table " + tableName);
        }
    }
}
