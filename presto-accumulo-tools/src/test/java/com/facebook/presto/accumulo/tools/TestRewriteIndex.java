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
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.index.metrics.MetricCacheKey;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.InvalidParameterException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.accumulo.index.Indexer.NULL_BYTE;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.primitives.Bytes.concat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRewriteIndex
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();
    private static final DateTimeFormatter PARSER = ISODateTimeFormat.dateTimeParser();
    private static final Long TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.321+0000").getMillis();
    private static final Long SECOND_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:05.000+0000").getMillis();
    private static final Long MINUTE_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:04:00.000+0000").getMillis();
    private static final Long HOUR_TIMESTAMP = PARSER.parseDateTime("2001-08-22T03:00:00.000+0000").getMillis();
    private static final Long DAY_TIMESTAMP = PARSER.parseDateTime("2001-08-22T00:00:00.000+0000").getMillis();
    private static final Type TIMESTAMP_TYPE = TimestampType.TIMESTAMP;

    private static final byte[] AGE = bytes("age");
    private static final byte[] CF = bytes("cf");
    private static final byte[] FIRSTNAME = bytes("firstname");
    private static final byte[] SENDERS = bytes("arr");
    private static final byte[] BIRTHDAY = bytes("birthday");

    private static final byte[] M1_ROWID = encode(VARCHAR, "row1");
    private static final byte[] AGE_VALUE = encode(BIGINT, 27L);
    private static final byte[] BIRTHDAY_VALUE = encode(TIMESTAMP_TYPE, TIMESTAMP);
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
    private AccumuloConfig config;
    private ZooKeeperMetadataManager metadataManager;
    private AccumuloTable table;
    private Connector connector;
    private MetricsStorage metricsStorage;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        connector = TestUtils.getAccumuloConnector();
        config = TestUtils.getAccumuloConfig();
        metadataManager = new ZooKeeperMetadataManager(config, new TypeRegistry());

        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));
        metricsStorage = MetricsStorage.getDefault(connector);

        AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "");
        AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "");
        AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "");
        AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "");
        AccumuloColumnHandle c5 = new AccumuloColumnHandle("birthday", Optional.of("cf"), Optional.of("birthday"), TIMESTAMP_TYPE, 4, "");

        table = new AccumuloTable("default", "test_rewrite_index", ImmutableList.of(c1, c2, c3, c4, c5), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null, Optional.empty(), true, Optional.of("age,firstname,arr,birthday,firstname:birthday"));

        m1 = new Mutation(M1_ROWID);
        m1.put(CF, AGE, AGE_VALUE);
        m1.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m1.put(CF, SENDERS, M1_ARR_VALUE);
        m1.put(CF, BIRTHDAY, BIRTHDAY_VALUE);

        ColumnVisibility visibility1 = new ColumnVisibility("private");
        m2v = new Mutation(M2_ROWID);
        m2v.put(CF, AGE, visibility1, AGE_VALUE);
        m2v.put(CF, FIRSTNAME, visibility1, M2_FNAME_VALUE);
        m2v.put(CF, SENDERS, visibility1, M2_ARR_VALUE);
        m2v.put(CF, BIRTHDAY, visibility1, BIRTHDAY_VALUE);

        ColumnVisibility visibility2 = new ColumnVisibility("moreprivate");
        m3v = new Mutation(M3_ROWID);
        m3v.put(CF, AGE, visibility2, AGE_VALUE);
        m3v.put(CF, FIRSTNAME, visibility2, M3_FNAME_VALUE);
        m3v.put(CF, SENDERS, visibility2, M3_ARR_VALUE);
        m3v.put(CF, BIRTHDAY, visibility2, BIRTHDAY_VALUE);
    }

    @BeforeMethod
    public void setup()
            throws Exception
    {
        metadataManager.createTableMetadata(table);

        connector.tableOperations().create(table.getFullTableName());
        connector.tableOperations().create(table.getIndexTableName());
        metricsStorage.create(table);
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        SchemaTableName schemaTableName = new SchemaTableName(table.getSchema(), table.getTable());
        if (metadataManager.getTable(schemaTableName) != null) {
            metadataManager.deleteTableMetadata(schemaTableName);
        }

        if (connector.tableOperations().exists(table.getFullTableName())) {
            connector.tableOperations().delete(table.getFullTableName());
        }

        if (connector.tableOperations().exists(table.getIndexTableName())) {
            connector.tableOperations().delete(table.getIndexTableName());
        }

        metricsStorage.drop(table);
    }

    @Test
    public void testNoConfig()
            throws Exception
    {
        RewriteIndex tool = new RewriteIndex();
        tool.setSchema(table.getSchema());
        tool.setTableName(table.getTable());
        assertTrue(tool.exec() != 0, "Expected non-zero exit code");
    }

    @Test
    public void testNoSchema()
            throws Exception
    {
        RewriteIndex tool = new RewriteIndex();
        tool.setConfig(config);
        tool.setTableName(table.getTable());
        assertTrue(tool.exec() != 0, "Expected non-zero exit code");
    }

    @Test
    public void testNoTable()
            throws Exception
    {
        RewriteIndex tool = new RewriteIndex();
        tool.setConfig(config);
        tool.setSchema(table.getSchema());
        assertTrue(tool.exec() != 0, "Expected non-zero exit code");
    }

    @Test
    public void testFullData()
            throws Exception
    {
        writeData();
        writeIndex();
        writeMetrics(false);
        exec();
        scanTable();
    }

    @Test
    public void testNoIndex()
            throws Exception
    {
        writeData();
        writeMetrics(true);
        exec();
        scanTable();
    }

    @Test
    public void testNoMetrics()
            throws Exception
    {
        writeData();
        writeIndex();
        exec();
        scanTable();
    }

    @Test
    public void testNoIndexOrMetrics()
            throws Exception
    {
        writeData();
        exec();
        scanTable();
    }

    @Test
    public void testColumnSelection()
            throws Exception
    {
        writeData();

        exec(false, true, Optional.of(ImmutableList.of("age", "birthday", "firstname")));

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row3", "moreprivate", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "moreprivate")), 0);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private")), 0);
    }

    @Test
    public void testColumnSelectionWithSomeMetrics()
            throws Exception
    {
        writeData();
        writeMetrics(true);

        exec(false, true, Optional.of(ImmutableList.of("age", "birthday", "firstname")));

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row3", "moreprivate", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private")), 1);
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testColumnDoesNotExist()
            throws Exception
    {
        exec(false, true, Optional.of(ImmutableList.of("doesnotexist")));
    }

    @Test(expectedExceptions = InvalidParameterException.class)
    public void testColumnIsRowID()
            throws Exception
    {
        exec(false, true, Optional.of(ImmutableList.of("id")));
    }

    @Test
    public void testDryRun()
            throws Exception
    {
        writeData();
        writeExtraIndexEntries();
        exec(true);

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row4", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row5", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row4", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row5", "private", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row4", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row5", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row4", "");
        assertKeyValuePair(iter.next(), concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row4", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row5", "private", "");
        assertKeyValuePair(iter.next(), concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row5", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row4", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row4", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row5", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row5", "private", "");
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private")), 1);
    }

    @Test
    public void testAddOnly()
            throws Exception
    {
        writeData();
        writeExtraIndexEntries();
        exec(false, false, Optional.empty());

        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row4", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row5", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row4", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row5", "private", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row4", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row5", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row4", "");
        assertKeyValuePair(iter.next(), concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row4", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row5", "private", "");
        assertKeyValuePair(iter.next(), concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row5", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row4", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row4", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row5", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row6", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row5", "private", "");
        assertFalse(iter.hasNext());
        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private")), 1);
    }

    @Test
    public void testExtraIndexAndMetricEntries()
            throws Exception
    {
        writeData();
        writeIndex();
        writeExtraIndexEntries();
        writeMetrics(false);

        exec();

        scanTable();
    }

    public void writeData()
            throws Exception
    {
        BatchWriter writer = connector.createBatchWriter(table.getFullTableName(), new BatchWriterConfig());
        writer.addMutations(ImmutableList.of(m1, m2v, m3v));
        writer.close();
    }

    public void writeIndex()
            throws Exception
    {
        BatchWriter writer = connector.createBatchWriter(table.getIndexTableName(), new BatchWriterConfig());
        Indexer indexer = new Indexer(connector, table, writer, table.getMetricsStorageInstance(connector).newWriter(table));
        indexer.index(ImmutableList.of(m1, m2v, m3v));
        writer.close();
    }

    private void writeExtraIndexEntries()
            throws Exception
    {
        Mutation m4 = new Mutation("row4");
        m4.put(CF, AGE, AGE_VALUE);
        m4.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m4.put(CF, SENDERS, M1_ARR_VALUE);
        m4.put(CF, BIRTHDAY, BIRTHDAY_VALUE);

        Mutation m5v = new Mutation("row5");
        m5v.put(CF, AGE, new ColumnVisibility("private"), AGE_VALUE);
        m5v.put(CF, FIRSTNAME, new ColumnVisibility("private"), M2_FNAME_VALUE);
        m5v.put(CF, SENDERS, new ColumnVisibility("private"), M2_ARR_VALUE);
        m5v.put(CF, BIRTHDAY, new ColumnVisibility("private"), BIRTHDAY_VALUE);

        Mutation m6v = new Mutation("row6");
        m6v.put(CF, AGE, new ColumnVisibility("moreprivate"), AGE_VALUE);
        m6v.put(CF, FIRSTNAME, new ColumnVisibility("moreprivate"), M3_FNAME_VALUE);
        m6v.put(CF, SENDERS, new ColumnVisibility("moreprivate"), M3_ARR_VALUE);
        m6v.put(CF, BIRTHDAY, new ColumnVisibility("moreprivate"), BIRTHDAY_VALUE);

        BatchWriter writer = connector.createBatchWriter(table.getIndexTableName(), new BatchWriterConfig());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, writer, metricsWriter);
        indexer.index(ImmutableList.of(m4, m5v, m6v));
        metricsWriter.close();
        writer.close();
    }

    public void writeMetrics(boolean dropIndex)
            throws Exception
    {
        BatchWriter writer = connector.createBatchWriter(table.getIndexTableName(), new BatchWriterConfig());
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        Indexer indexer = new Indexer(connector, table, writer, metricsWriter);
        indexer.index(ImmutableList.of(m1, m2v, m3v));
        metricsWriter.close();

        // Even though we don't close the batch writer, index mutations may have been written as well
        // We'll recreate the index table just in case
        if (dropIndex) {
            connector.tableOperations().delete(table.getIndexTableName());
            connector.tableOperations().create(table.getIndexTableName());
        }
    }

    public void scanTable()
            throws Exception
    {
        Scanner scan = connector.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), BIRTHDAY_VALUE, "cf_birthday", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "cf_firstname-cf_birthday", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());

        scan.close();

        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_age", AGE_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday", BIRTHDAY_VALUE, "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsd", encode(TIMESTAMP_TYPE, DAY_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsh", encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tsm", encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_birthday_tss", encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP), "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getNumRowsInTable(table.getSchema(), table.getTable()), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "abc", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M1_FNAME_VALUE)), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M2_FNAME_VALUE, "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname", M3_FNAME_VALUE, "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M1_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M2_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday", concat(M3_FNAME_VALUE, NULL_BYTE, BIRTHDAY_VALUE), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsd", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, DAY_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsh", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, HOUR_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tsm", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, MINUTE_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M1_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)))), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M2_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "private")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_firstname-cf_birthday_tss", concat(M3_FNAME_VALUE, NULL_BYTE, encode(TIMESTAMP_TYPE, SECOND_TIMESTAMP)), "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "def", "moreprivate")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private")), 2);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "ghi", "private", "moreprivate")), 3);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "jkl", "moreprivate")), 1);
        assertEquals(metricsStorage.newReader().getCardinality(mck("cf_arr", "mno", "private")), 1);
    }

    private void exec()
            throws Exception
    {
        exec(false, true, Optional.empty());
    }

    private void exec(boolean dryRun)
            throws Exception
    {
        exec(dryRun, true, Optional.empty());
    }

    private void exec(boolean dryRun, boolean delete, Optional<List<String>> columns)
            throws Exception
    {
        RewriteIndex tool = new RewriteIndex();
        tool.setConfig(config);
        tool.setAuthorizations(new Authorizations("private", "moreprivate"));
        tool.setSchema(table.getSchema());
        tool.setTableName(table.getTable());
        tool.setDryRun(dryRun);
        tool.setDelete(delete);
        tool.setColumns(columns);
        assertTrue(tool.exec() == 0, "Expected exit code of value zero");
    }

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private MetricCacheKey mck(String family, String range, String... auths)
    {
        return mck(family, range.getBytes(UTF_8), auths);
    }

    private MetricCacheKey mck(String family, byte[] range, String... auths)
    {
        return new MetricCacheKey(table.getSchema(), table.getTable(), new Text(family), new Authorizations(auths), new Range(new Text(range)));
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row);
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().toString(), value);
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

    private static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }
}
