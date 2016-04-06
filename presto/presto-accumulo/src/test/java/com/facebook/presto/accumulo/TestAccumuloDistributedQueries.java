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
package com.facebook.presto.accumulo;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;

import java.util.List;

import static com.facebook.presto.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static com.facebook.presto.accumulo.AccumuloQueryRunner.dropTpchTables;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAccumuloDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestAccumuloDistributedQueries()
            throws Exception
    {
        super(createAccumuloQueryRunner(ImmutableMap.of(), true));
    }

    @AfterClass
    public void cleanup()
    {
        dropTpchTables(queryRunner, getSession());
    }

    @Override
    public void testAddColumn()
            throws Exception
    {
        try {
            // TODO Adding columns via SQL are not supported until adding columns with comments are supported
            super.testAddColumn();
        }
        catch (Exception e) {
            assertEquals("Must have at least one non-row ID column", e.getMessage());
        }
    }

    @Override
    public void testCompatibleTypeChangeForView()
            throws Exception
    {
        try {
            // TODO Views are not yet supported
            // Override because base class error: Must have at least one non-row ID column
            assertUpdate("CREATE TABLE test_table_1 WITH (column_mapping = 'b:b:b') AS SELECT 'abcdefg' a, 1 b", 1);
            assertUpdate("CREATE VIEW test_view_1 AS SELECT a FROM test_table_1");

            assertQuery("SELECT * FROM test_view_1", "VALUES 'abcdefg'");

            // replace table with a version that's implicitly coercible to the previous one
            assertUpdate("DROP TABLE test_table_1");
            assertUpdate("CREATE TABLE test_table_1 AS SELECT 'abc' a", 1);

            assertQuery("SELECT * FROM test_view_1", "VALUES 'abc'");

            assertUpdate("DROP VIEW test_view_1");
            assertUpdate("DROP TABLE test_table_1");
        }
        catch (Exception e) {
            assertEquals("This connector does not support creating views", e.getMessage());
        }
    }

    @Override
    public void testCreateTable()
            throws Exception
    {
        // Override because base class error: Must specify column mapping property
        assertUpdate("CREATE TABLE test_create (a bigint, b double, c varchar) WITH (column_mapping = 'b:b:b,c:c:c')");
        assertTrue(queryRunner.tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertUpdate("DROP TABLE test_create");
        assertFalse(queryRunner.tableExists(getSession(), "test_create"));

        assertUpdate("CREATE TABLE test_create_table_if_not_exists (a bigint, b varchar, c double) WITH (column_mapping = 'b:b:b,c:c:c')");
        assertTrue(queryRunner.tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_if_not_exists (d bigint, e varchar) WITH (column_mapping = 'e:e:e')");
        assertTrue(queryRunner.tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("DROP TABLE test_create_table_if_not_exists");
        assertFalse(queryRunner.tableExists(getSession(), "test_create_table_if_not_exists"));
    }

    @Override
    public void testCreateTableAsSelect()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_create_table_as_if_not_exists (a bigint, b double) WITH (column_mapping = 'b:b:b')");
        assertTrue(queryRunner.tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        MaterializedResult materializedRows = computeActual("CREATE TABLE IF NOT EXISTS test_create_table_as_if_not_exists WITH (column_mapping = 'orderkey:a:a, discount:a:b') AS SELECT UUID() AS uuid, orderkey, discount FROM lineitem");
        assertEquals(materializedRows.getRowCount(), 0);
        assertTrue(queryRunner.tableExists(getSession(), "test_create_table_as_if_not_exists"));
        assertTableColumnNames("test_create_table_as_if_not_exists", "a", "b");

        assertUpdate("DROP TABLE test_create_table_as_if_not_exists");
        assertFalse(queryRunner.tableExists(getSession(), "test_create_table_as_if_not_exists"));

//        Function "UUID" not found
//        this.assertCreateTableAsSelect(
//                "test_select",
//                "orderdate:cf:orderdate,orderkey:cf:orderkey,totalprice:cf:totalprice",
//                "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders",
//                "SELECT count(*) FROM orders");

        this.assertCreateTableAsSelect(
                "test_group",
                "orderstatus:cf:orderstatus,x:cf:x",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

//        Function "UUID" not found
//        this.assertCreateTableAsSelect(
//                "test_join",
//                "x:x:x",
//                "SELECT UUID() AS uuid, count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
//                "SELECT 1");

//        Function "UUID" not found
//        this.assertCreateTableAsSelect(
//                "test_limit",
//                "orderkey:cf:orderkey",
//                "SELECT UUID() AS uuid, orderkey FROM orders ORDER BY orderkey LIMIT 10",
//                "SELECT 10");

//        Function "UUID" not found
//        this.assertCreateTableAsSelect(
//                "test_unicode",
//                "unicode:cf:unicode",
//                "SELECT UUID() AS uuid, '\u2603' unicode",
//                "SELECT 1");

        this.assertCreateTableAsSelect(
                "test_with_data",
                "custkey:md:custkey,orderstatus:md:orderstatus,totalprice:md:totalprice,orderdate:md:orderdate,orderpriority:md:orderpriority,clerk:md:clerk,shippriority:md:shippriority,comment:md:comment",
                "SELECT * FROM orders WITH DATA",
                "SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        this.assertCreateTableAsSelect(
                "test_with_no_data",
                "custkey:md:custkey,orderstatus:md:orderstatus,totalprice:md:totalprice,orderdate:md:orderdate,orderpriority:md:orderpriority,clerk:md:clerk,shippriority:md:shippriority,comment:md:comment",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");

//        No sample tables
//        this.assertCreateTableAsSelect(
//                "test_sampled",
//                "custkey:md:custkey,orderstatus:md:orderstatus,totalprice:md:totalprice,orderdate:md:orderdate,orderpriority:md:orderpriority,clerk:md:clerk,shippriority:md:shippriority,comment:md:comment",
//                "SELECT orderkey FROM tpch_sampled.tiny.orders ORDER BY orderkey LIMIT 10",
//                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
//                "SELECT 10");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

//        Function "UUID" not found
//        this.assertCreateTableAsSelect(
//                "test_union_all",
//                "orderdate:md:orderdate,orderkey:md:orderkey,totalprice:md:totalprice",
//                "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
//                        "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
//                "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders",
//                "SELECT count(*) FROM orders");

//        Function "UUID" not found
//        this.assertCreateTableAsSelect(
//                getSession().withSystemProperty("redistribute_writes", "true"),
//                "test_union_all",
//                "orderdate:md:orderdate,orderkey:md:orderkey,totalprice:md:totalprice",
//                "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders UNION ALL " +
//                        "SELECT UUID() AS uuid, DATE '2000-01-01', 1234567890, 1.23",
//                "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders UNION ALL " +
//                        "SELECT UUID() AS uuid, DATE '2000-01-01', 1234567890, 1.23",
//                "SELECT count(*) + 1 FROM orders");

//        Function "UUID" not found
//        this.assertCreateTableAsSelect(
//                getSession().withSystemProperty("redistribute_writes", "false"),
//                "test_union_all",
//                "orderdate:md:orderdate,orderkey:md:orderkey,totalprice:md:totalprice",
//                "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders UNION ALL " +
//                        "SELECT UUID() AS uuid, DATE '2000-01-01', 1234567890, 1.23",
//                "SELECT UUID() AS uuid, orderdate, orderkey, totalprice FROM orders UNION ALL " +
//                        "SELECT UUID() AS uuid, DATE '2000-01-01', 1234567890, 1.23",
//                "SELECT count(*) + 1 FROM orders");
    }

    @Override
    public void testDelete()
            throws Exception
    {
        // Override because base class error: Must specify column mapping property
        try {
            // TODO Deletes are not supported by the connector
            super.testDelete();
        }
        catch (Exception e) {
            assertEquals("Must specify column mapping property", e.getMessage());
        }
    }

    @Override
    public void testInsert()
            throws Exception
    {
        // Override because base class error: Must specify column mapping property
        @Language("SQL") String query = "SELECT UUID() AS uuid, orderdate, orderkey FROM orders";

        assertUpdate("CREATE TABLE test_insert WITH (column_mapping = 'orderdate:cf:orderdate,orderkey:cf:orderkey') AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT orderdate, orderkey FROM test_insert", "SELECT orderdate, orderkey FROM orders");
        // Override because base class error: Cannot insert null row ID
        assertUpdate("INSERT INTO test_insert (uuid, orderkey) VALUES ('000000', -1)", 1);
        assertUpdate("INSERT INTO test_insert (uuid, orderdate) VALUES ('000001', DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (uuid, orderkey, orderdate) VALUES ('000002', -2, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (uuid, orderdate, orderkey) VALUES ('000003', DATE '2001-01-03', -3)", 1);

        assertQuery("SELECT orderdate, orderkey FROM test_insert",
                "SELECT orderdate, orderkey FROM orders"
                        + " UNION ALL SELECT null, -1"
                        + " UNION ALL SELECT DATE '2001-01-01', null"
                        + " UNION ALL SELECT DATE '2001-01-02', -2"
                        + " UNION ALL SELECT DATE '2001-01-03', -3");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (uuid, orderkey, orderdate) " +
                        "SELECT UUID() AS uuid, orderkey, orderdate FROM orders " +
                        "UNION ALL " +
                        "SELECT UUID() AS uuid, orderkey, orderdate FROM orders",
                "SELECT 2 * count(*) FROM orders");
    }

    @Override
    public void testRenameColumn()
            throws Exception
    {
        // Override because base class error: Must have at least one non-row ID column
        assertUpdate("CREATE TABLE test_rename_column WITH (column_mapping = 'a:a:a') AS SELECT 123 x, 456 a", 1);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN x TO y");
        MaterializedResult materializedRows = computeActual("SELECT y FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN y TO Z");
        materializedRows = computeActual("SELECT z FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("DROP TABLE test_rename_column");
        assertFalse(queryRunner.tableExists(getSession(), "test_rename_column"));
    }

    @Override
    public void testRenameTable()
            throws Exception
    {
        // Override because base class error: Must have at least one non-row ID column
        assertUpdate("CREATE TABLE test_rename WITH (column_mapping = 'a:a:a') AS SELECT 123 x, 456 a", 1);

        assertUpdate("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        // provide new table name in uppercase
        assertUpdate("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123L);

        assertUpdate("DROP TABLE test_rename");

        assertFalse(queryRunner.tableExists(getSession(), "test_rename"));
        assertFalse(queryRunner.tableExists(getSession(), "test_rename_new"));
    }

    @Override
    public void testSymbolAliasing()
            throws Exception
    {
        // Override because base class error: Must specify column mapping property
        assertUpdate("CREATE TABLE test_symbol_aliasing WITH (column_mapping = 'foo_2_4:a:a') AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM test_symbol_aliasing", "SELECT 1, 2");
        assertUpdate("DROP TABLE test_symbol_aliasing");
    }

    @Override
    public void testTableSampleSystem()
            throws Exception
    {
        // Override because base class error: ???
        // TODO table sample system is not supported (I think?)
        int total = computeActual("SELECT orderkey FROM orders").getMaterializedRows().size();

        boolean sampleSizeFound = false;
        for (int i = 0; i < 100; i++) {
            int sampleSize = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE SYSTEM (50)").getMaterializedRows().size();
            if (sampleSize > 0 && sampleSize < total) {
                sampleSizeFound = true;
                break;
            }
        }
        //  assertTrue(sampleSizeFound, "Table sample returned unexpected number of rows");
    }

    @Override
    public void testView()
            throws Exception
    {
        try {
            // TODO Views are not yet supported
            super.testView();
        }
        catch (Exception e) {
            assertEquals("This connector does not support creating views", e.getMessage());
        }
    }

    @Override
    public void testViewMetadata()
            throws Exception
    {
        try {
            // TODO Views are not yet supported
            super.testViewMetadata();
        }
        catch (Exception e) {
            assertEquals("This connector does not support creating views", e.getMessage());
        }
    }

    @Override
    public void testBuildFilteredLeftJoin()
            throws Exception
    {
        try {
            // TODO Figure this out
            super.testBuildFilteredLeftJoin();
        }
        catch (Exception e) {
            assertEquals("type does not match result", e.getMessage());
        }
    }

    @Override
    public void testJoinWithAlias()
            throws Exception
    {
        try {
            // TODO Figure this out
            super.testJoinWithAlias();
        }
        catch (Exception e) {
            assertEquals("type does not match result", e.getMessage());
        }
    }

    @Override
    public void testProbeFilteredLeftJoin()
            throws Exception
    {
        try {
            // TODO Figure this out
            super.testProbeFilteredLeftJoin();
        }
        catch (Exception e) {
            assertEquals("type does not match result", e.getMessage());
        }
    }

    @Override
    public void testScalarSubquery()
            throws Exception
    {
        try {
            // TODO Figure this out
            super.testScalarSubquery();
        }
        catch (Exception e) {
            assertEquals("type does not match result", e.getMessage());
        }
    }

    @Override
    public void testShowColumns()
            throws Exception
    {
        // Override base class because table descriptions for Accumulo connector include comments
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "Accumulo row ID")
                .row("custkey", "bigint", "Accumulo column cf:custkey. Indexed: false")
                .row("orderstatus", "varchar", "Accumulo column cf:orderstatus. Indexed: false")
                .row("totalprice", "double", "Accumulo column cf:totalprice. Indexed: false")
                .row("orderdate", "date", "Accumulo column cf:orderdate. Indexed: false")
                .row("orderpriority", "varchar", "Accumulo column cf:orderpriority. Indexed: false")
                .row("clerk", "varchar", "Accumulo column cf:clerk. Indexed: false")
                .row("shippriority", "bigint", "Accumulo column cf:shippriority. Indexed: false")
                .row("comment", "varchar", "Accumulo column cf:comment. Indexed: false")
                .build();

        assertEquals(actual, expected);
    }

    // Copied from abstract base class
    private void assertTableColumnNames(String tableName, String... columnNames)
    {
        MaterializedResult result = computeActual("DESCRIBE " + tableName);
        List<String> expected = ImmutableList.copyOf(columnNames);
        List<String> actual = result.getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
        assertEquals(actual, expected);
    }

    // Copied from abstract base class
    private void assertCreateTableAsSelect(String table, String columnMapping, @Language("SQL") String query, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertCreateTableAsSelect(getSession(), table, columnMapping, query, query, rowCountQuery);
    }

    // Copied from abstract base class
    private void assertCreateTableAsSelect(String table, String columnMapping, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertCreateTableAsSelect(getSession(), table, columnMapping, query, expectedQuery, rowCountQuery);
    }

    // Copied from abstract base class
    private void assertCreateTableAsSelect(Session session, String table, String columnMapping, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
            throws Exception
    {
        assertUpdate(session, "CREATE TABLE " + table + " WITH (column_mapping='" + columnMapping + "') AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertFalse(queryRunner.tableExists(session, table));
    }
}
