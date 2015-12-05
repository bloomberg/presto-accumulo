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
package bloomberg.presto.accumulo;

import static bloomberg.presto.accumulo.MetadataUtil.CATALOG_CODEC;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Test(singleThreaded = true)
public class TestAccumuloMetadata {
    private static final String CONNECTOR_ID = "TEST";
    private static final AccumuloTableHandle TEST_TABLE_HANDLE = new AccumuloTableHandle(
            CONNECTOR_ID, "default", "foo");
    private AccumuloMetadata metadata;

    @BeforeMethod
    public void setUp() throws Exception {
        AccumuloConfig config = new AccumuloConfig();
        AccumuloClient client = new AccumuloClient(config, CATALOG_CODEC);
        metadata = new AccumuloMetadata(new AccumuloConnectorId(CONNECTOR_ID),
                client);
    }

    @Test(enabled = false)
    public void testListSchemaNames() {
        assertEquals(metadata.listSchemaNames(SESSION),
                ImmutableSet.of("default"));
    }

    @Test(enabled = false)
    public void testGetTableHandle() {
        assertEquals(
                metadata.getTableHandle(SESSION,
                        new SchemaTableName("default", "foo")),
                TEST_TABLE_HANDLE);
    }

    @Test(enabled = false)
    public void testGetColumnHandles() {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, TEST_TABLE_HANDLE),
                ImmutableMap.of("cf1__cq1", new AccumuloColumnHandle(
                        CONNECTOR_ID, "cf1__cq1", VARCHAR, 0)));

        // unknown table
        try {
            metadata.getColumnHandles(SESSION, new AccumuloTableHandle(
                    CONNECTOR_ID, "unknown", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        } catch (TableNotFoundException expected) {
        }
        try {
            metadata.getColumnHandles(SESSION, new AccumuloTableHandle(
                    CONNECTOR_ID, "example", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        } catch (TableNotFoundException expected) {
        }
    }

    @Test(enabled = false)
    public void getTableMetadata() {
        // known table
        ConnectorTableMetadata tableMetadata = metadata
                .getTableMetadata(SESSION, TEST_TABLE_HANDLE);
        assertEquals(tableMetadata.getTable(),
                new SchemaTableName("default", "foo"));
        assertEquals(tableMetadata.getColumns(), ImmutableList
                .of(new ColumnMetadata("cf1__cq1", VARCHAR, false)));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION,
                new AccumuloTableHandle(CONNECTOR_ID, "unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION,
                new AccumuloTableHandle(CONNECTOR_ID, "example", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION,
                new AccumuloTableHandle(CONNECTOR_ID, "unknown", "numbers")));
    }

    @Test(enabled = false)
    public void testListTables() {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, null)),
                ImmutableSet.of(new SchemaTableName("default", "foo")));

        // specific schema
        assertEquals(
                ImmutableSet.copyOf(metadata.listTables(SESSION, "default")),
                ImmutableSet.of(new SchemaTableName("default", "foo")));

        // unknown schema
        assertEquals(
                ImmutableSet.copyOf(metadata.listTables(SESSION, "unknown")),
                ImmutableSet.of());
    }

    @Test(enabled = false)
    public void getColumnMetadata() {
        assertEquals(
                metadata.getColumnMetadata(SESSION, TEST_TABLE_HANDLE,
                        new AccumuloColumnHandle(CONNECTOR_ID, "cf1__cq1",
                                VARCHAR, 0)),
                new ColumnMetadata("cf1__cq1", VARCHAR, false));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // ExampleTableHandle and ExampleColumnHandle passed in. This is on
        // because
        // it is not possible for the Presto Metadata system to create the
        // handles
        // directly.
    }

    @Test(enabled = false, expectedExceptions = PrestoException.class)
    public void testCreateTable() {
        metadata.createTable(SESSION,
                new ConnectorTableMetadata(new SchemaTableName("example",
                        "foo"),
                ImmutableList.of(new ColumnMetadata("text", VARCHAR, false))));
    }

    @Test(enabled = false, expectedExceptions = PrestoException.class)
    public void testDropTableTable() {
        metadata.dropTable(SESSION, TEST_TABLE_HANDLE);
    }
}
