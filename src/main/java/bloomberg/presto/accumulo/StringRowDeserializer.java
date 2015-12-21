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

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

import bloomberg.presto.accumulo.metadata.AccumuloTableMetadataManager;
import io.airlift.log.Logger;

public class StringRowDeserializer implements AccumuloRowDeserializer {
    private static final Logger LOG = Logger.get(StringRowDeserializer.class);
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, Object> columnValues = new HashMap<>();
    private Text rowId = new Text(), cf = new Text(), cq = new Text(),
            value = new Text();

    @Override
    public void setMapping(String name, String fam, String qual) {
        columnValues.put(name, null);
        Map<String, String> q2pc = f2q2pc.get(fam);
        if (q2pc == null) {
            q2pc = new HashMap<>();
            f2q2pc.put(fam, q2pc);
        }

        q2pc.put(qual, name);
        LOG.debug(String.format("Added mapping for presto col %s, %s:%s", name,
                fam, qual));
    }

    @Override
    public void deserialize(Entry<Key, Value> row) throws IOException {
        columnValues.clear();

        SortedMap<Key, Value> decodedRow = WholeRowIterator
                .decodeRow(row.getKey(), row.getValue());

        decodedRow.entrySet().iterator().next().getKey().getRow(rowId);
        columnValues.put(AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME,
                rowId.toString());

        for (Entry<Key, Value> kvp : decodedRow.entrySet()) {
            kvp.getKey().getColumnFamily(cf);
            kvp.getKey().getColumnQualifier(cq);
            value.set(kvp.getValue().get());
            columnValues.put(f2q2pc.get(cf.toString()).get(cq.toString()),
                    value.toString());
        }
    }

    @Override
    public boolean isNull(String name) {
        return columnValues.get(name) == null;
    }

    @Override
    public boolean getBoolean(String name) {
        return Boolean.parseBoolean(getFieldValue(name));
    }

    @Override
    public Date getDate(String name) {
        return new Date(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public double getDouble(String name) {
        return Double.parseDouble(getFieldValue(name));
    }

    @Override
    public long getLong(String name) {
        return Long.parseLong(getFieldValue(name));
    }

    @Override
    public Object getObject(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String name) {
        return new Time(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public Timestamp getTimestamp(String name) {
        return new Timestamp(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public byte[] getVarbinary(String name) {
        return getFieldValue(name).getBytes();
    }

    @Override
    public String getVarchar(String name) {
        return getFieldValue(name);
    }

    private String getFieldValue(String name) {
        return columnValues.get(name).toString();
    }
}
