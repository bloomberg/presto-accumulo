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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;

import static bloomberg.presto.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static bloomberg.presto.accumulo.AccumuloQueryRunner.dropTpchTables;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestAccumuloIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestAccumuloIntegrationSmokeTest()
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
    public void testDescribeTable()
            throws Exception
    {
        // Override base class because table descriptions for Accumulo connector include comments
        MaterializedResult actualColumns = computeActual("DESC ORDERS").toJdbcTypes();
        assertEquals(actualColumns, getExpectedTableDescription());
    }

    @Override
    public void testViewAccessControl()
            throws Exception
    {
        try {
            // Views are not yet supported
            super.testViewAccessControl();
        }
        catch (Exception e) {
            assertEquals("This connector does not support creating views", e.getMessage());
        }
    }

    private MaterializedResult getExpectedTableDescription()
    {
        return MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR, VARCHAR, VARCHAR)
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
    }
}
