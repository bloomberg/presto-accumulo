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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestAccumuloClient {
    @Test
    public void testMetadata() throws Exception {
        AccumuloClient client = new AccumuloClient(new AccumuloConfig(),
                CATALOG_CODEC);
        assertEquals(client.getSchemaNames(), ImmutableSet.of("foo"));
        assertEquals(client.getTableNames("foo"), ImmutableSet.of("bar"));

        AccumuloTable table = client.getTable("foo", "bar");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "bar");
        assertEquals(table.getColumns(),
                ImmutableList.of(new AccumuloColumn("cf1", "cq1", VARCHAR)));
    }
}
