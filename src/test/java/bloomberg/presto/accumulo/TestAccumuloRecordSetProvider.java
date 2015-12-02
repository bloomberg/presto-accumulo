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

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

public class TestAccumuloRecordSetProvider {

    @Test
    public void testGetRecordSet() throws Exception {
        AccumuloRecordSetProvider recordSetProvider = new AccumuloRecordSetProvider(
                new AccumuloConnectorId("test"));
        RecordSet recordSet = recordSetProvider.getRecordSet(SESSION,
                new AccumuloSplit("test", "foo", "bar"),
                ImmutableList.of(new AccumuloColumnHandle("test", "cf1__cq1",
                        VARCHAR, 0)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        List<String> data = new ArrayList<>();
        while (cursor.advanceNextPosition()) {
            data.add(cursor.getSlice(0).toStringUtf8());
        }

        assertEquals(data,
                ImmutableList.<String> builder().add("value").build());
    }

    //
    // Start http server for testing
    //

    @BeforeClass
    public void setUp() throws Exception {
    }

    @AfterClass
    public void tearDown() throws Exception {
    }
}
