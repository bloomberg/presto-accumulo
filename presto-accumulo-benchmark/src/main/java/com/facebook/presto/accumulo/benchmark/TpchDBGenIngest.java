/**
 * Copyright 2016 Bloomberg L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.benchmark;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.io.AccumuloPageSink;
import com.facebook.presto.accumulo.metadata.AccumuloMetadataManager;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.accumulo.model.RowSchema;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
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
import java.util.Optional;
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
            .addColumn("custkey", Optional.empty(), Optional.empty(), BIGINT)
            .addColumn("name", Optional.of("md"), Optional.of("name"), VARCHAR)
            .addColumn("address", Optional.of("md"), Optional.of("address"), VARCHAR)
            .addColumn("nationkey", Optional.of("md"), Optional.of("nationkey"), BIGINT)
            .addColumn("phone", Optional.of("md"), Optional.of("phone"), VARCHAR)
            .addColumn("acctbal", Optional.of("md"), Optional.of("acctbal"), DOUBLE)
            .addColumn("mktsegment", Optional.of("md"), Optional.of("mktsegment"), VARCHAR, true)
            .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private static final RowSchema LINEITEM_SCHEMA = RowSchema.newRowSchema()
            .addColumn("uuid", Optional.empty(), Optional.empty(), VARCHAR)
            .addColumn("orderkey", Optional.of("md"), Optional.of("orderkey"), BIGINT)
            .addColumn("partkey", Optional.of("md"), Optional.of("partkey"), BIGINT)
            .addColumn("suppkey", Optional.of("md"), Optional.of("suppkey"), BIGINT)
            .addColumn("linenumber", Optional.of("md"), Optional.of("linenumber"), BIGINT)
            .addColumn("quantity", Optional.of("md"), Optional.of("quantity"), BIGINT, true)
            .addColumn("extendedprice", Optional.of("md"), Optional.of("extendedprice"), DOUBLE)
            .addColumn("discount", Optional.of("md"), Optional.of("discount"), DOUBLE, true)
            .addColumn("tax", Optional.of("md"), Optional.of("tax"), DOUBLE)
            .addColumn("returnflag", Optional.of("md"), Optional.of("returnflag"), VARCHAR, true)
            .addColumn("linestatus", Optional.of("md"), Optional.of("linestatus"), VARCHAR)
            .addColumn("shipdate", Optional.of("md"), Optional.of("shipdate"), DATE, true)
            .addColumn("commitdate", Optional.of("md"), Optional.of("commitdate"), DATE)
            .addColumn("receiptdate", Optional.of("md"), Optional.of("receiptdate"), DATE, true)
            .addColumn("shipinstruct", Optional.of("md"), Optional.of("shipinstruct"), VARCHAR, true)
            .addColumn("shipmode", Optional.of("md"), Optional.of("shipmode"), VARCHAR, true)
            .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private static final RowSchema NATION_SCHEMA =
            RowSchema.newRowSchema()
                    .addColumn("nationkey", Optional.empty(), Optional.empty(), BIGINT)
                    .addColumn("name", Optional.of("md"), Optional.of("name"), VARCHAR, true)
                    .addColumn("regionkey", Optional.of("md"), Optional.of("regionkey"), BIGINT)
                    .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private static final RowSchema ORDERS_SCHEMA = RowSchema.newRowSchema()
            .addColumn("orderkey", Optional.empty(), Optional.empty(), BIGINT)
            .addColumn("custkey", Optional.of("md"), Optional.of("custkey"), BIGINT)
            .addColumn("orderstatus", Optional.of("md"), Optional.of("orderstatus"), VARCHAR)
            .addColumn("totalprice", Optional.of("md"), Optional.of("totalprice"), DOUBLE)
            .addColumn("orderdate", Optional.of("md"), Optional.of("orderdate"), DATE, true)
            .addColumn("orderpriority", Optional.of("md"), Optional.of("orderpriority"), VARCHAR)
            .addColumn("clerk", Optional.of("md"), Optional.of("clerk"), VARCHAR)
            .addColumn("shippriority", Optional.of("md"), Optional.of("shippriority"), BIGINT)
            .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private static final RowSchema PART_SCHEMA = RowSchema.newRowSchema()
            .addColumn("partkey", Optional.empty(), Optional.empty(), BIGINT)
            .addColumn("name", Optional.of("md"), Optional.of("name"), VARCHAR)
            .addColumn("mfgr", Optional.of("md"), Optional.of("mfgr"), VARCHAR)
            .addColumn("brand", Optional.of("md"), Optional.of("brand"), VARCHAR, true)
            .addColumn("type", Optional.of("md"), Optional.of("type"), VARCHAR, true)
            .addColumn("size", Optional.of("md"), Optional.of("size"), BIGINT, true)
            .addColumn("container", Optional.of("md"), Optional.of("container"), VARCHAR, true)
            .addColumn("retailprice", Optional.of("md"), Optional.of("retailprice"), DOUBLE)
            .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private static final RowSchema PARTSUPP_SCHEMA =
            RowSchema.newRowSchema()
                    .addColumn("uuid", Optional.empty(), Optional.empty(), VARCHAR)
                    .addColumn("partkey", Optional.of("md"), Optional.of("partkey"), BIGINT, true)
                    .addColumn("suppkey", Optional.of("md"), Optional.of("suppkey"), BIGINT)
                    .addColumn("availqty", Optional.of("md"), Optional.of("availqty"), BIGINT)
                    .addColumn("supplycost", Optional.of("md"), Optional.of("supplycost"), DOUBLE)
                    .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private static final RowSchema REGION_SCHEMA =
            RowSchema.newRowSchema()
                    .addColumn("regionkey", Optional.empty(), Optional.empty(), BIGINT)
                    .addColumn("name", Optional.of("md"), Optional.of("name"), VARCHAR, true)
                    .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private static final RowSchema SUPPLIER_SCHEMA = RowSchema.newRowSchema()
            .addColumn("suppkey", Optional.empty(), Optional.empty(), BIGINT)
            .addColumn("name", Optional.of("md"), Optional.of("name"), VARCHAR, true)
            .addColumn("address", Optional.of("md"), Optional.of("address"), VARCHAR)
            .addColumn("nationkey", Optional.of("md"), Optional.of("nationkey"), BIGINT)
            .addColumn("phone", Optional.of("md"), Optional.of("phone"), VARCHAR)
            .addColumn("acctbal", Optional.of("md"), Optional.of("acctbal"), DOUBLE)
            .addColumn("comment", Optional.of("md"), Optional.of("comment"), VARCHAR);

    private TpchDBGenIngest() {}

    public static void run(AccumuloConfig accConfig, String schema, File dbgenDir)
            throws Exception
    {
        if (!dbgenDir.exists()) {
            throw new FileNotFoundException("Given datagen directory does not exist");
        }

        List<File> dataFiles = Arrays.asList(dbgenDir.listFiles()).stream()
                .filter(x -> x.getName().endsWith(".tbl")).collect(Collectors.toList());

        if (dataFiles.isEmpty()) {
            throw new FileNotFoundException("No table files found in datagen directory");
        }

        AccumuloMetadataManager mgr = AccumuloMetadataManager.getDefault(accConfig);

        ZooKeeperInstance inst =
                new ZooKeeperInstance(accConfig.getInstance(), accConfig.getZooKeepers());
        Connector conn = inst.getConnector(accConfig.getUsername(),
                new PasswordToken(accConfig.getPassword()));
        AccumuloRowSerializer serializer = AccumuloRowSerializer.getDefault();
        for (File df : dataFiles) {
            String tableName = FilenameUtils.removeExtension(df.getName());
            String fullTableName = schema + '.' + tableName;

            RowSchema rowSchema = schemaFromFile(tableName);
            String rowIdName = rowIdFromFile(tableName);

            AccumuloTable table = new AccumuloTable(schema, tableName, rowSchema.getColumns(),
                    rowIdName, true, serializer.getClass().getCanonicalName(), null);

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
                System.out.println(
                        String.format("Created index table %s", table.getIndexTableName()));
                conn.tableOperations().setLocalityGroups(table.getIndexTableName(), groups);

                conn.tableOperations().create(table.getMetricsTableName());
                conn.tableOperations().setLocalityGroups(table.getMetricsTableName(), groups);
                for (IteratorSetting s : Indexer.getMetricIterators(table)) {
                    conn.tableOperations().attachIterator(table.getMetricsTableName(), s);
                }

                System.out.println(String.format("Created index metrics table %s",
                        table.getMetricsTableName()));

                indexer = new Indexer(conn,
                        conn.securityOperations().getUserAuthorizations(accConfig.getUsername()),
                        table, new BatchWriterConfig());
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

            System.out.println(String.format("Reading rows from file %s, writing to table %s", df,
                    fullTableName));
            String line;
            int numRows = 0;
            int numIdxRows = 0;
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
