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
package bloomberg.presto.accumulo.io;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

import bloomberg.presto.accumulo.PrestoType;
import bloomberg.presto.accumulo.metadata.AccumuloTableMetadataManager;
import io.airlift.log.Logger;

public class LexicoderRowDeserializer implements AccumuloRowDeserializer {
    private static final Logger LOG = Logger
            .get(LexicoderRowDeserializer.class);
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, byte[]> columnValues = new HashMap<>();
    private Text rowId = new Text(), cf = new Text(), cq = new Text(),
            value = new Text();
    private static Map<PrestoType, Lexicoder<?>> lexicoderMap = null;

    static {
        if (lexicoderMap == null) {
            lexicoderMap = new HashMap<>();

            // This for loop is here so a lexicoder for a type isn't missed
            for (PrestoType t : PrestoType.values()) {
                switch (t) {
                case BIGINT:
                case DATE:
                case TIME:
                case TIMESTAMP:
                    lexicoderMap.put(t, new LongLexicoder());
                    break;
                case BOOLEAN:
                    lexicoderMap.put(t, new BooleanLexicoder());
                    break;
                case DOUBLE:
                    lexicoderMap.put(t, new DoubleLexicoder());
                    break;
                case VARBINARY:
                    lexicoderMap.put(t, new BytesLexicoder());
                    break;
                case VARCHAR:
                    lexicoderMap.put(t, new StringLexicoder());
                    break;
                default:
                    throw new RuntimeException("No lexicoder for type " + t);
                }
            }
        }
    }

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
                rowId.copyBytes());

        for (Entry<Key, Value> kvp : decodedRow.entrySet()) {
            kvp.getKey().getColumnFamily(cf);
            kvp.getKey().getColumnQualifier(cq);
            value.set(kvp.getValue().get());
            columnValues.put(f2q2pc.get(cf.toString()).get(cq.toString()),
                    value.copyBytes());
        }
    }

    @Override
    public boolean isNull(String name) {
        return columnValues.get(name) == null;
    }

    @Override
    public boolean getBoolean(String name) {
        return (Boolean) lexicoderMap.get(PrestoType.BOOLEAN)
                .decode(getFieldValue(name));
    }

    @Override
    public Date getDate(String name) {
        return new Date((Long) lexicoderMap.get(PrestoType.DATE)
                .decode(getFieldValue(name)));
    }

    @Override
    public double getDouble(String name) {
        return (Double) lexicoderMap.get(PrestoType.DOUBLE)
                .decode(getFieldValue(name));
    }

    @Override
    public long getLong(String name) {
        return (Long) lexicoderMap.get(PrestoType.BIGINT)
                .decode(getFieldValue(name));
    }

    @Override
    public Object getObject(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String name) {
        return new Time((Long) lexicoderMap.get(PrestoType.TIME)
                .decode(getFieldValue(name)));
    }

    @Override
    public Timestamp getTimestamp(String name) {
        return new Timestamp((Long) lexicoderMap.get(PrestoType.TIMESTAMP)
                .decode(getFieldValue(name)));
    }

    @Override
    public byte[] getVarbinary(String name) {
        return (byte[]) lexicoderMap.get(PrestoType.VARBINARY)
                .decode(getFieldValue(name));
    }

    @Override
    public String getVarchar(String name) {
        return (String) lexicoderMap.get(PrestoType.VARCHAR)
                .decode(getFieldValue(name));
    }

    private byte[] getFieldValue(String name) {
        return columnValues.get(name);
    }
}
