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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

public class TestAccumuloRecordSet {
    @Test(enabled = false)
    public void testGetColumnTypes() throws Exception {
        RecordSet recordSet = new AccumuloRecordSet(
                new AccumuloSplit("test", "schema", "table"),
                ImmutableList.of(
                        new AccumuloColumnHandle("test", "cf1__cq1", VARCHAR,
                                0),
                        new AccumuloColumnHandle("test", "cf2__cq2", BIGINT,
                                1)));
        assertEquals(recordSet.getColumnTypes(),
                ImmutableList.of(VARCHAR, BIGINT));

        recordSet = new AccumuloRecordSet(
                new AccumuloSplit("test", "foo", "table"),
                ImmutableList.of(
                        new AccumuloColumnHandle("test", "cf2__cq2", BIGINT, 1),
                        new AccumuloColumnHandle("test", "cf1__cq1", VARCHAR,
                                0)));
        assertEquals(recordSet.getColumnTypes(),
                ImmutableList.of(BIGINT, VARCHAR));

        recordSet = new AccumuloRecordSet(
                new AccumuloSplit("test", "foo", "table"),
                ImmutableList.of(
                        new AccumuloColumnHandle("test", "cf2__cq2", BIGINT, 1),
                        new AccumuloColumnHandle("test", "cf2__cq2", BIGINT, 1),
                        new AccumuloColumnHandle("test", "cf1__cq1", VARCHAR,
                                0)));
        assertEquals(recordSet.getColumnTypes(),
                ImmutableList.of(BIGINT, BIGINT, VARCHAR));

        recordSet = new AccumuloRecordSet(
                new AccumuloSplit("test", "foo", "table"),
                ImmutableList.<AccumuloColumnHandle> of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test(enabled = false)
    public void testCursorSimple() throws Exception {
        RecordSet recordSet = new AccumuloRecordSet(
                new AccumuloSplit("test", "foo", "table"),
                ImmutableList.of(new AccumuloColumnHandle("test", "cf1__cq1",
                        VARCHAR, 0)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), VARCHAR);

        List<String> data = new ArrayList<>();
        while (cursor.advanceNextPosition()) {
            data.add(cursor.getSlice(0).toStringUtf8());
            assertFalse(cursor.isNull(0));
        }

        assertEquals(data,
                ImmutableList.<String> builder().add("value").build());
    }

    //
    // TODO: your code should also have tests for all types that you support and
    // for the state machine of your cursor
    //

    @BeforeClass
    public void setUp() throws Exception {
    }

    @AfterClass
    public void tearDown() throws Exception {

    }
}
