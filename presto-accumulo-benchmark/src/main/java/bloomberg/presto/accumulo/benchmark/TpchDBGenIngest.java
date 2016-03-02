package bloomberg.presto.accumulo.benchmark;

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.index.Indexer;
import bloomberg.presto.accumulo.io.AccumuloPageSink;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.metadata.AccumuloTable;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TpchDBGenIngest
{
    private static final char DELIMITER = '|';
    private static final String CUSTOMER_ROW_ID = "custkey";
    private static final String LINEITEM_ROW_ID = "uuid";
    private static final String NATION_ROW_ID = "nationkey";
    private static final String ORDERS_ROW_ID = "orderkey";
    private static final String PART_ROW_ID = "partkey";
    private static final String PARTSUPP_ROW_ID = "uuid";
    private static final String REGION_ROW_ID = "regionkey";
    private static final String SUPPLIER_ROW_ID = "suppkey";

    private static final RowSchema CUSTOMER_SCHEMA = RowSchema.newRowSchema()
            .addColumn("custkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("address", "md", "address", VARCHAR)
            .addColumn("nationkey", "md", "nationkey", BIGINT)
            .addColumn("phone", "md", "phone", VARCHAR)
            .addColumn("acctbal", "md", "acctbal", DOUBLE)
            .addColumn("mktsegment", "md", "mktsegment", VARCHAR, true)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema LINEITEM_SCHEMA = RowSchema.newRowSchema()
            .addColumn("uuid", null, null, VARCHAR)
            .addColumn("orderkey", "md", "orderkey", BIGINT)
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

    private static final RowSchema NATION_SCHEMA = RowSchema.newRowSchema()
            .addColumn("nationkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR, true)
            .addColumn("regionkey", "md", "regionkey", BIGINT)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema ORDERS_SCHEMA = RowSchema.newRowSchema()
            .addColumn("orderkey", null, null, BIGINT)
            .addColumn("custkey", "md", "custkey", BIGINT)
            .addColumn("orderstatus", "md", "orderstatus", VARCHAR)
            .addColumn("totalprice", "md", "totalprice", DOUBLE)
            .addColumn("orderdate", "md", "orderdate", DATE, true)
            .addColumn("orderpriority", "md", "orderpriority", VARCHAR)
            .addColumn("clerk", "md", "clerk", VARCHAR)
            .addColumn("shippriority", "md", "shippriority", BIGINT)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema PART_SCHEMA = RowSchema.newRowSchema()
            .addColumn("partkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR)
            .addColumn("mfgr", "md", "mfgr", VARCHAR)
            .addColumn("brand", "md", "brand", VARCHAR, true)
            .addColumn("type", "md", "type", VARCHAR, true)
            .addColumn("size", "md", "size", BIGINT, true)
            .addColumn("container", "md", "container", VARCHAR, true)
            .addColumn("retailprice", "md", "retailprice", DOUBLE)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema PARTSUPP_SCHEMA = RowSchema.newRowSchema()
            .addColumn("uuid", null, null, VARCHAR)
            .addColumn("partkey", "md", "partkey", BIGINT, true)
            .addColumn("suppkey", "md", "suppkey", BIGINT)
            .addColumn("availqty", "md", "availqty", BIGINT)
            .addColumn("supplycost", "md", "supplycost", DOUBLE)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema REGION_SCHEMA = RowSchema.newRowSchema()
            .addColumn("regionkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR, true)
            .addColumn("comment", "md", "comment", VARCHAR);

    private static final RowSchema SUPPLIER_SCHEMA = RowSchema.newRowSchema()
            .addColumn("suppkey", null, null, BIGINT)
            .addColumn("name", "md", "name", VARCHAR, true)
            .addColumn("address", "md", "address", VARCHAR)
            .addColumn("nationkey", "md", "nationkey", BIGINT)
            .addColumn("phone", "md", "phone", VARCHAR)
            .addColumn("acctbal", "md", "acctbal", DOUBLE)
            .addColumn("comment", "md", "comment", VARCHAR);

    public static void run(AccumuloConfig accConfig, String schema, File dbgenDir)
            throws Exception
    {

        if (!dbgenDir.exists()) {
            throw new FileNotFoundException("Given datagen directory does not exist");
        }

        List<File> dataFiles = Arrays.asList(dbgenDir.listFiles()).stream().filter(x -> x.getName().endsWith(".tbl")).collect(Collectors.toList());

        if (dataFiles.isEmpty()) {
            throw new FileNotFoundException("No table files found in datagen directory");
        }

        AccumuloMetadataManager mgr = AccumuloMetadataManager.getDefault(accConfig);

        ZooKeeperInstance inst = new ZooKeeperInstance(accConfig.getInstance(), accConfig.getZooKeepers());
        Connector conn = inst.getConnector(accConfig.getUsername(), new PasswordToken(accConfig.getPassword()));
        AccumuloRowSerializer serializer = AccumuloRowSerializer.getDefault();
        for (File df : dataFiles) {
            String tableName = FilenameUtils.removeExtension(df.getName());
            String fullTableName = schema + '.' + tableName;

            RowSchema rowSchema = schemaFromFile(tableName);
            String rowIdName = rowIdFromFile(tableName);

            AccumuloTable table = new AccumuloTable(schema, tableName, rowSchema.getColumns(), rowIdName, true, serializer.getClass().getCanonicalName());

            mgr.createTableMetadata(table);

            if (!conn.namespaceOperations().exists(schema)) {
                conn.namespaceOperations().create(schema);
            }

            conn.tableOperations().create(fullTableName);

            System.out.println(String.format("Created table %s", table));

            BufferedReader rdr = new BufferedReader(new FileReader(df));
            BatchWriterConfig bwc = new BatchWriterConfig();
            BatchWriter wrtr = conn.createBatchWriter(fullTableName, bwc);

            final Indexer indexer;
            if (table.isIndexed()) {
                Map<String, Set<Text>> groups = Indexer.getLocalityGroups(table);

                conn.tableOperations().create(table.getIndexTableName());
                System.out.println(String.format("Created index table %s", table.getIndexTableName()));
                conn.tableOperations().setLocalityGroups(table.getIndexTableName(), groups);

                conn.tableOperations().create(table.getMetricsTableName());
                conn.tableOperations().setLocalityGroups(table.getMetricsTableName(), groups);
                for (IteratorSetting s : Indexer.getMetricIterators(table)) {
                    conn.tableOperations().attachIterator(table.getMetricsTableName(), s);
                }

                System.out.println(String.format("Created index metrics table %s", table.getMetricsTableName()));

                indexer = new Indexer(conn, conn.securityOperations().getUserAuthorizations(accConfig.getUsername()), table, new BatchWriterConfig());
            }
            else {
                indexer = null;
            }

            List<AccumuloColumnHandle> columns = rowSchema.getColumns();
            Collections.sort(columns, new Comparator<AccumuloColumnHandle>()
            {
                @Override
                public int compare(AccumuloColumnHandle o1, AccumuloColumnHandle o2)
                {
                    return Integer.compare(o1.getOrdinal(), o2.getOrdinal());
                }
            });

            int rowIdOrdinal = -1;
            for (AccumuloColumnHandle ach : columns) {
                if (ach.getName().equals(table.getRowId())) {
                    rowIdOrdinal = ach.getOrdinal();
                    break;
                }
            }

            System.out.println(String.format("Reading rows from file %s, writing to table %s", df, fullTableName));
            String line;
            int numRows = 0, numIdxRows = 0;
            boolean hasUuid = hasUuid(tableName);
            while ((line = rdr.readLine()) != null) {

                // append a UUID to the line if this table has one
                if (hasUuid) {
                    line = UUID.randomUUID().toString() + DELIMITER + line;
                }

                Row r = Row.fromString(rowSchema, line, DELIMITER);

                Mutation m = AccumuloPageSink.toMutation(r, rowIdOrdinal, columns, serializer);

                wrtr.addMutation(m);

                if (indexer != null) {
                    indexer.index(m);
                }

                ++numRows;
            }

            wrtr.flush();
            wrtr.close();

            if (indexer != null) {
                indexer.close();
            }

            rdr.close();
            System.out.println(String.format("Wrote %d rows, %d index rows", numRows, numIdxRows));
        }
    }

    private static boolean hasUuid(String tableName)
    {
        switch (tableName) {
            case "lineitem":
            case "partsupp":
                return true;
            default:
                return false;
        }
    }

    private static String rowIdFromFile(String tableName)
    {
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
                throw new InvalidParameterException("Unknown row ID for table " + tableName);
        }
    }

    private static RowSchema schemaFromFile(String tableName)
    {
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
                throw new InvalidParameterException("Unknown schema for table " + tableName);
        }
    }
}
