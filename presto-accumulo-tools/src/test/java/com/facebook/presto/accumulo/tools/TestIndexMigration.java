/*
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
package com.facebook.presto.accumulo.tools;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.metrics.MetricCacheKey;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.index.storage.ShardedIndexStorage;
import com.facebook.presto.accumulo.io.PrestoBatchWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.SparkSession;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage.ENCODER;
import static com.facebook.presto.accumulo.metadata.AccumuloTable.DEFAULT_NUM_SHARDS;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.minicluster.MemoryUnit.GIGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestIndexMigration
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    private static final Logger LOG = Logger.get(TestIndexMigration.class);
    private static final AccumuloConfig CONFIG = new AccumuloConfig();
    private static final DateTimeFormatter PARSER = ISODateTimeFormat.dateTimeParser();
    private static final byte[] TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis());
    private static final byte[] SECOND_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis());
    private static final byte[] MINUTE_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis());
    private static final byte[] HOUR_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis());
    private static final byte[] DAY_TIMESTAMP_VALUE = encode(TimestampType.TIMESTAMP, PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis());

    private static final byte[] ROW_ID_COLUMN = PrestoBatchWriter.ROW_ID_COLUMN.copyBytes();
    private static final byte[] AGE = bytes("age");
    private static final byte[] CF = bytes("cf");
    private static final byte[] FIRSTNAME = bytes("firstname");
    private static final byte[] BORN = bytes("born");
    private static final byte[] SENDERS = bytes("arr");

    private static final byte[] M1_ROWID = encode(VARCHAR, "row1");
    private static final byte[] AGE_VALUE = encode(BIGINT, 27L);
    private static final byte[] M1_FNAME_VALUE = encode(VARCHAR, "alice");
    private static final byte[] M1_ARR_VALUE = encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("abc", "def", "ghi")));

    private static final byte[] M2_ROWID = encode(VARCHAR, "row2");
    private static final byte[] M2_FNAME_VALUE = encode(VARCHAR, "bob");
    private static final byte[] M2_ARR_VALUE = encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("ghi", "mno", "abc")));

    private static final byte[] M3_ROWID = encode(VARCHAR, "row3");
    private static final byte[] M3_FNAME_VALUE = encode(VARCHAR, "carol");
    private static final byte[] M3_ARR_VALUE = encode(new ArrayType(VARCHAR), getBlockFromArray(VARCHAR, ImmutableList.of("def", "ghi", "jkl")));

    private Mutation m1 = null;
    private Mutation m2v = null;
    private Mutation m3v = null;
    private AccumuloTable table;
    private AccumuloTable newTable;
    private Connector connector;
    private MetricsStorage metricsStorage;
    private ZooKeeperMetadataManager metadataManager;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        CONFIG.setUsername("root");
        CONFIG.setPassword("secret");
        File macDir = Files.createTempDirectory("mac-").toFile();
        LOG.info("MAC is enabled, starting MiniAccumuloCluster at %s", macDir);

        MiniAccumuloConfig config = new MiniAccumuloConfig(macDir, "secret");
        config.setZooKeeperPort(21810);
        config.setDefaultMemory(2, GIGABYTE);

        // Start MAC and connect to it
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config);
        accumulo.start();

        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        connector = instance.getConnector("root", new PasswordToken("secret"));

        CONFIG.setInstance(instance.getInstanceName());
        CONFIG.setZooKeepers(instance.getZooKeepers());

        LOG.info("Connection to MAC instance %s at %s established, user %s password %s", accumulo.getInstanceName(), accumulo.getZooKeepers(), "root", "secret");

        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));
        metricsStorage = MetricsStorage.getDefault(connector);
        metadataManager = new ZooKeeperMetadataManager(CONFIG, new TypeRegistry());

        AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "");
        AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "");
        AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "");
        AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "");
        AccumuloColumnHandle c5 = new AccumuloColumnHandle("born", Optional.of("cf"), Optional.of("born"), TimestampType.TIMESTAMP, 4, "");

        table = new AccumuloTable("default", "presto_batch_writer_test_table", ImmutableList.of(c1, c2, c3, c4, c5), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.empty(), true, Optional.empty());
        newTable = new AccumuloTable("default", "presto_batch_writer_new_table", ImmutableList.of(c1, c2, c3, c4, c5), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.empty(), true, Optional.of("age,arr,born,firstname"));

        m1 = new Mutation(M1_ROWID);
        m1.put(ROW_ID_COLUMN, ROW_ID_COLUMN, M1_ROWID);
        m1.put(CF, AGE, AGE_VALUE);
        m1.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m1.put(CF, SENDERS, M1_ARR_VALUE);
        m1.put(CF, BORN, TIMESTAMP_VALUE);

        ColumnVisibility visibility1 = new ColumnVisibility("private");
        m2v = new Mutation(M2_ROWID);
        m2v.put(ROW_ID_COLUMN, ROW_ID_COLUMN, visibility1, M2_ROWID);
        m2v.put(CF, AGE, visibility1, AGE_VALUE);
        m2v.put(CF, FIRSTNAME, visibility1, M2_FNAME_VALUE);
        m2v.put(CF, SENDERS, visibility1, M2_ARR_VALUE);
        m2v.put(CF, BORN, visibility1, TIMESTAMP_VALUE);

        ColumnVisibility visibility2 = new ColumnVisibility("moreprivate");
        m3v = new Mutation(M3_ROWID);
        m3v.put(ROW_ID_COLUMN, ROW_ID_COLUMN, visibility2, M3_ROWID);
        m3v.put(CF, AGE, visibility2, AGE_VALUE);
        m3v.put(CF, FIRSTNAME, visibility2, M3_FNAME_VALUE);
        m3v.put(CF, SENDERS, visibility2, M3_ARR_VALUE);
        m3v.put(CF, BORN, visibility2, TIMESTAMP_VALUE);
    }

    @BeforeMethod
    public void setup()
            throws Exception
    {
        connector.tableOperations().create(table.getFullTableName());

        metadataManager.createTableMetadata(table);

        connector.tableOperations().create(newTable.getFullTableName());
        for (IndexColumn indexColumn : newTable.getParsedIndexColumns()) {
            if (!connector.tableOperations().exists(indexColumn.getIndexTable())) {
                connector.tableOperations().create(indexColumn.getIndexTable());
            }
        }

        metricsStorage.create(newTable);
        metadataManager.createTableMetadata(newTable);
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        if (connector.tableOperations().exists(table.getFullTableName())) {
            connector.tableOperations().delete(table.getFullTableName());
        }

        metadataManager.deleteTableMetadata(table.getSchemaTableName());

        if (connector.tableOperations().exists(newTable.getFullTableName())) {
            connector.tableOperations().delete(newTable.getFullTableName());
        }

        for (IndexColumn indexColumn : newTable.getParsedIndexColumns()) {
            if (connector.tableOperations().exists(indexColumn.getIndexTable())) {
                connector.tableOperations().delete(indexColumn.getIndexTable());
            }
        }

        metricsStorage.drop(newTable);
        metadataManager.deleteTableMetadata(newTable.getSchemaTableName());

        File output = new File("output");
        if (output.exists()) {
            FileUtils.forceDelete(output);
        }
    }

    @Test
    public void testOnlineMigration()
            throws Exception
    {
        runMigration(false);
    }

    @Test
    public void testOfflineMigration()
            throws Exception
    {
        runMigration(true);
    }

    private void runMigration(boolean isOfflineScan)
            throws Exception
    {
        PrestoBatchWriter prestoBatchWriter = new PrestoBatchWriter(connector, connector.securityOperations().getUserAuthorizations("root"), table);
        prestoBatchWriter.addMutations(ImmutableList.of(m1, m2v, m3v));
        prestoBatchWriter.close();

        if (isOfflineScan) {
            connector.tableOperations().offline(table.getFullTableName(), true);
        }

        IndexMigration migration = new IndexMigration();
        migration.setSparkSession(
                SparkSession.builder()
                        .appName("IndexMigration")
                        .master("local[*]")
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .config("spark.kryo.registrator", "com.facebook.presto.accumulo.tools.IndexMigrationRegistrator")
                        .getOrCreate());

        migration.exec(
                CONFIG,
                connector.getInstance().getInstanceName(),
                connector.getInstance().getZooKeepers(),
                "root",
                "secret",
                "presto_batch_writer_test_table",
                "presto_batch_writer_new_table",
                new Authorizations("private", "moreprivate"),
                20,
                isOfflineScan,
                "output");

        Scanner scan = connector.createScanner(newTable.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "___ROW___", "___ROW___", "", "row1");
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", "", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", "", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "born", "", TIMESTAMP_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", "", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "___ROW___", "___ROW___", "private", "row2");
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", "private", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", "private", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "born", "private", TIMESTAMP_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", "private", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "___ROW___", "___ROW___", "moreprivate", "row3");
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", "moreprivate", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", "moreprivate", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "born", "moreprivate", TIMESTAMP_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", "moreprivate", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());
        scan.close();

        scan = createIndexScanner(0);
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age_car", "car", "private", ENCODER.encode(1L));
        assertFalse(iter.hasNext());
        scan.close();

        scan = createIndexScanner(1);
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr_car", "car", "private", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr_car", "car", "private", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr_car", "car", "private", ENCODER.encode(1L));
        assertFalse(iter.hasNext());
        scan.close();

        scan = createIndexScanner(2);
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), shard(HOUR_TIMESTAMP_VALUE), "cf_born_tsh_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(HOUR_TIMESTAMP_VALUE), "cf_born_tsh_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(HOUR_TIMESTAMP_VALUE), "cf_born_tsh_car", "car", "private", ENCODER.encode(1L));

        assertKeyValuePair(iter.next(), shard(SECOND_TIMESTAMP_VALUE), "cf_born_tss_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(SECOND_TIMESTAMP_VALUE), "cf_born_tss_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(SECOND_TIMESTAMP_VALUE), "cf_born_tss_car", "car", "private", ENCODER.encode(1L));

        assertKeyValuePair(iter.next(), shard(DAY_TIMESTAMP_VALUE), "cf_born_tsd_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(DAY_TIMESTAMP_VALUE), "cf_born_tsd_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(DAY_TIMESTAMP_VALUE), "cf_born_tsd_car", "car", "private", ENCODER.encode(1L));

        assertKeyValuePair(iter.next(), shard(TIMESTAMP_VALUE), "cf_born", "row1", "", "");
        assertKeyValuePair(iter.next(), shard(TIMESTAMP_VALUE), "cf_born", "row2", "private", "");
        assertKeyValuePair(iter.next(), shard(TIMESTAMP_VALUE), "cf_born", "row3", "moreprivate", "");

        assertKeyValuePair(iter.next(), shard(TIMESTAMP_VALUE), "cf_born_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(TIMESTAMP_VALUE), "cf_born_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(TIMESTAMP_VALUE), "cf_born_car", "car", "private", ENCODER.encode(1L));

        assertKeyValuePair(iter.next(), shard(MINUTE_TIMESTAMP_VALUE), "cf_born_tsm_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(MINUTE_TIMESTAMP_VALUE), "cf_born_tsm_car", "car", "moreprivate", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), shard(MINUTE_TIMESTAMP_VALUE), "cf_born_tsm_car", "car", "private", ENCODER.encode(1L));
        assertFalse(iter.hasNext());
        scan.close();

        scan = createIndexScanner(3);
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname_car", "car", "", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname_car", "car", "private", ENCODER.encode(1L));
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname_car", "car", "moreprivate", ENCODER.encode(1L));
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(0).getIndexTable(), "cf_age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(newTable), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(1).getIndexTable(), "cf_arr", "abc", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(3).getIndexTable(), "cf_firstname", M1_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(3).getIndexTable(), "cf_firstname", M2_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(3).getIndexTable(), "cf_firstname", M3_FNAME_VALUE, "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(1).getIndexTable(), "cf_arr", "def", "private", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(1).getIndexTable(), "cf_arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(1).getIndexTable(), "cf_arr", "jkl", "private", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck(newTable.getParsedIndexColumns().get(1).getIndexTable(), "cf_arr", "mno", "private", "moreprivate")), 1);
    }

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private MetricCacheKey mck(String indexTable, String family, String range, String... auths)
    {
        return mck(indexTable, family, range.getBytes(UTF_8), auths);
    }

    private MetricCacheKey mck(String indexTable, String family, byte[] range, String... auths)
    {
        return new MetricCacheKey(indexTable, new Text(family), new Authorizations(auths), new Range(new Text(range)), metricsStorage);
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String cv, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getKey().getColumnVisibility().toString(), cv);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().toString(), value);
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String cv, byte[] value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertEquals(e.getKey().getColumnVisibility().toString(), cv);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().get(), value, format("Expected %s but found %s", new String(e.getValue().get(), UTF_8), new String(value, UTF_8)));
    }

    private Scanner createIndexScanner(int index)
            throws TableNotFoundException
    {
        return connector.createScanner(newTable.getParsedIndexColumns().get(index).getIndexTable(), new Authorizations("private", "moreprivate"));
    }

    private static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }

    private static byte[] shard(byte[] bytes)
    {
        return new ShardedIndexStorage(DEFAULT_NUM_SHARDS).encode(bytes);
    }
}
