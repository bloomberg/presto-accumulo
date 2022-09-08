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
/*
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
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoBatchWriter
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    public static final AccumuloConfig CONFIG = new AccumuloConfig();

    private static final byte[] AGE = bytes("age");
    private static final byte[] CF = bytes("cf");
    private static final byte[] FIRSTNAME = bytes("firstname");
    private static final byte[] SENDERS = bytes("arr");
    private static final byte[] METRICS_TABLE = bytes("___METRICS_TABLE___");

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
    private AccumuloClient accumuloClient;
    private PrestoBatchWriter prestoBatchWriter;
    private ZooKeeperMetadataManager metadataManager;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        accumuloClient = TestUtils.getAccumuloClient();
        accumuloClient.securityOperations().changeUserAuthorizations("root", new Authorizations("private", "moreprivate", "foo", "bar", "xyzzy"));

        AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "", false);
        AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "", true);
        AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "", true);
        AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "", true);

        table = new AccumuloTable("default", "presto_batch_writer_test_table", ImmutableList.of(c1, c2, c3, c4), "id", false, LexicoderRowSerializer.class.getCanonicalName(), null);

        m1 = new Mutation(M1_ROWID);
        m1.put(CF, AGE, AGE_VALUE);
        m1.put(CF, FIRSTNAME, M1_FNAME_VALUE);
        m1.put(CF, SENDERS, M1_ARR_VALUE);

        ColumnVisibility visibility1 = new ColumnVisibility("private");
        m2v = new Mutation(M2_ROWID);
        m2v.put(CF, AGE, visibility1, AGE_VALUE);
        m2v.put(CF, FIRSTNAME, visibility1, M2_FNAME_VALUE);
        m2v.put(CF, SENDERS, visibility1, M2_ARR_VALUE);

        ColumnVisibility visibility2 = new ColumnVisibility("moreprivate");
        m3v = new Mutation(M3_ROWID);
        m3v.put(CF, AGE, visibility2, AGE_VALUE);
        m3v.put(CF, FIRSTNAME, visibility2, M3_FNAME_VALUE);
        m3v.put(CF, SENDERS, visibility2, M3_ARR_VALUE);
    }

    @BeforeMethod
    public void setup()
        throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {

        metadataManager = new ZooKeeperMetadataManager(CONFIG, FunctionAndTypeManager.createTestFunctionAndTypeManager());
        if (metadataManager.getTable(table.getSchemaTableName()) == null) {
            metadataManager.createTableMetadata(table);
        }

        if (!accumuloClient.tableOperations().exists(table.getFullTableName())) {
            accumuloClient.tableOperations().create(table.getFullTableName());
        }

        if (!accumuloClient.tableOperations().exists(table.getIndexTableName())) {
            accumuloClient.tableOperations().create(table.getIndexTableName());
        }

        if (!accumuloClient.tableOperations().exists(table.getMetricsTableName())) {
            accumuloClient.tableOperations().create(table.getMetricsTableName());
        }

        // Set locality groups on index and metrics table
        Map<String, Set<Text>> indexGroups = Indexer.getLocalityGroups(table);
        accumuloClient.tableOperations().setLocalityGroups(table.getIndexTableName(), indexGroups);
        accumuloClient.tableOperations().setLocalityGroups(table.getMetricsTableName(), indexGroups);

        // Attach iterators to metrics table
        for (IteratorSetting setting : Indexer.getMetricIterators(table)) {
            if (!accumuloClient.tableOperations().listIterators(table.getMetricsTableName()).containsKey(setting.getName())) {
                accumuloClient.tableOperations().attachIterator(table.getMetricsTableName(), setting);
            }
        }

        this.prestoBatchWriter = new PrestoBatchWriter();
        prestoBatchWriter.setConfig(CONFIG);
        prestoBatchWriter.setSchema(table.getSchema());
        prestoBatchWriter.setTableName(table.getTable());
        prestoBatchWriter.init();
    }

    @AfterMethod
    public void cleanup()
            throws Exception
    {
        if (accumuloClient.tableOperations().exists(table.getFullTableName())) {
            accumuloClient.tableOperations().delete(table.getFullTableName());
        }

        if (accumuloClient.tableOperations().exists(table.getIndexTableName())) {
            accumuloClient.tableOperations().delete(table.getIndexTableName());
        }

        if (accumuloClient.tableOperations().exists(table.getMetricsTableName())) {
            accumuloClient.tableOperations().delete(table.getMetricsTableName());
        }
        this.metadataManager.deleteTableMetadata(table.getSchemaTableName());
    }

    @Test
    public void testAddMutation()
            throws Exception
    {
        prestoBatchWriter.addMutation(m1);
        prestoBatchWriter.flush();

        Scanner scan = accumuloClient.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___card___", "1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___last_row___", "row1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertFalse(iter.hasNext());
        scan.close();

        prestoBatchWriter.addMutation(m2v);
        prestoBatchWriter.flush();

        scan = accumuloClient.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___card___", "2");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___last_row___", "row2");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "private", "1");
        assertFalse(iter.hasNext());
        scan.close();

        prestoBatchWriter.addMutation(m3v);
        prestoBatchWriter.close();

        scan = accumuloClient.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___card___", "3");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___last_row___", "row3");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "private", "1");
        assertFalse(iter.hasNext());
        scan.close();
    }

    @Test
    public void testAddMutations()
            throws Exception
    {
        prestoBatchWriter.addMutations(ImmutableList.of(m1, m2v, m3v));
        prestoBatchWriter.close();

        Scanner scan = accumuloClient.createScanner(table.getFullTableName(), new Authorizations("private", "moreprivate"));
        Iterator<Entry<Key, Value>> iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "arr", M1_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row1"), "cf", "firstname", M1_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "arr", M2_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row2"), "cf", "firstname", M2_FNAME_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "age", AGE_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "arr", M3_ARR_VALUE);
        assertKeyValuePair(iter.next(), bytes("row3"), "cf", "firstname", M3_FNAME_VALUE);
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getIndexTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row1", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row2", "private", "");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "row1", "");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "row2", "private", "");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row1", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row2", "private", "");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "row3", "moreprivate", "");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "row2", "private", "");
        assertFalse(iter.hasNext());
        scan.close();

        scan = accumuloClient.createScanner(table.getMetricsTableName(), new Authorizations("private", "moreprivate"));
        iter = scan.iterator();
        assertTrue(iter.hasNext());
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), AGE_VALUE, "cf_age", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___card___", "3");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___first_row___", "row1");
        assertKeyValuePair(iter.next(), METRICS_TABLE, "___rows___", "___last_row___", "row3");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("abc"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M1_FNAME_VALUE, "cf_firstname", "___card___", "1");
        assertKeyValuePair(iter.next(), M2_FNAME_VALUE, "cf_firstname", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), M3_FNAME_VALUE, "cf_firstname", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("def"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("ghi"), "cf_arr", "___card___", "private", "1");
        assertKeyValuePair(iter.next(), bytes("jkl"), "cf_arr", "___card___", "moreprivate", "1");
        assertKeyValuePair(iter.next(), bytes("mno"), "cf_arr", "___card___", "private", "1");
        assertFalse(iter.hasNext());
        scan.close();
    }

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row, format("%s does not match %s", new String(e.getKey().getRow().copyBytes()), new String(row)));
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().toString(), value);
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, byte[] value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row, format("%s does not match %s", new String(e.getKey().getRow().copyBytes()), new String(row)));
        assertEquals(e.getKey().getColumnFamily().toString(), cf);
        assertEquals(e.getKey().getColumnQualifier().toString(), cq);
        assertTrue(e.getKey().getTimestamp() > 0, "Timestamp is zero");
        assertEquals(e.getValue().get(), value);
    }

    private void assertKeyValuePair(Entry<Key, Value> e, byte[] row, String cf, String cq, String cv, String value)
    {
        assertEquals(e.getKey().getRow().copyBytes(), row, format("%s does not match %s", new String(e.getKey().getRow().copyBytes()), new String(row)));
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
