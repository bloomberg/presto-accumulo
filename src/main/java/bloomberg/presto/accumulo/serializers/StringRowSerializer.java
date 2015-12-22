package bloomberg.presto.accumulo.serializers;

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

public class StringRowSerializer implements AccumuloRowSerializer {
    private static final Logger LOG = Logger.get(StringRowSerializer.class);
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
    public void setBoolean(Text text, Boolean value) {
        text.set(value.toString().getBytes());
    }

    @Override
    public Date getDate(String name) {
        return new Date(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setDate(Text text, Date value) {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public double getDouble(String name) {
        return Double.parseDouble(getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value) {
        text.set(value.toString().getBytes());
    }

    @Override
    public long getLong(String name) {
        return Long.parseLong(getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value) {
        text.set(value.toString().getBytes());
    }

    @Override
    public Time getTime(String name) {
        return new Time(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value) {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public Timestamp getTimestamp(String name) {
        return new Timestamp(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value) {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public byte[] getVarbinary(String name) {
        return getFieldValue(name).getBytes();
    }

    @Override
    public void setVarbinary(Text text, byte[] value) {
        text.set(value);
    }

    @Override
    public String getVarchar(String name) {
        return getFieldValue(name);
    }

    @Override
    public void setVarchar(Text text, String value) {
        text.set(value.getBytes());
    }

    private String getFieldValue(String name) {
        return columnValues.get(name).toString();
    }
}
