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

import bloomberg.presto.accumulo.AccumuloColumn;
import bloomberg.presto.accumulo.AccumuloTable;

import com.facebook.presto.spi.ColumnMetadata;
import com.google.common.collect.ImmutableList;

import org.testng.annotations.Test;

import java.net.URI;

import static bloomberg.presto.accumulo.MetadataUtil.TABLE_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestAccumuloTable
{
    private final AccumuloTable exampleTable = new AccumuloTable("tableName",
            ImmutableList.of(new AccumuloColumn("a", VARCHAR), new AccumuloColumn("b", BIGINT)),
            ImmutableList.of(URI.create("file://table-1.json"), URI.create("file://table-2.json")));

    @Test
    public void testColumnMetadata()
    {
        assertEquals(exampleTable.getColumnsMetadata(), ImmutableList.of(
                new ColumnMetadata("a", VARCHAR, false),
                new ColumnMetadata("b", BIGINT, false)));
    }

    @Test
    public void testRoundTrip()
            throws Exception
    {
        String json = TABLE_CODEC.toJson(exampleTable);
        AccumuloTable exampleTableCopy = TABLE_CODEC.fromJson(json);

        assertEquals(exampleTableCopy.getName(), exampleTable.getName());
        assertEquals(exampleTableCopy.getColumns(), exampleTable.getColumns());
        assertEquals(exampleTableCopy.getSources(), exampleTable.getSources());
    }
}
