package bloomberg.presto.accumulo.serializers;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

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

@SuppressWarnings({ "rawtypes", "unchecked" })
public class LexicoderRowSerializer implements AccumuloRowSerializer {
    private static final Logger LOG = Logger.get(LexicoderRowSerializer.class);
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, byte[]> columnValues = new HashMap<>();
    private Text rowId = new Text(), cf = new Text(), cq = new Text(),
            value = new Text();
    private static Map<PrestoType, Lexicoder> lexicoderMap = null;

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
    public void setBoolean(Text text, Boolean value) {
        text.set(lexicoderMap.get(PrestoType.BOOLEAN).encode(value));
    }

    @Override
    public Date getDate(String name) {
        return new Date((Long) lexicoderMap.get(PrestoType.DATE)
                .decode(getFieldValue(name)));
    }

    @Override
    public void setDate(Text text, Date value) {
        text.set(lexicoderMap.get(PrestoType.DATE)
                .encode(TimeUnit.MILLISECONDS.toDays(value.getTime())));
    }

    @Override
    public double getDouble(String name) {
        return (Double) lexicoderMap.get(PrestoType.DOUBLE)
                .decode(getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value) {
        text.set(lexicoderMap.get(PrestoType.DOUBLE).encode(value));
    }

    @Override
    public long getLong(String name) {
        return (Long) lexicoderMap.get(PrestoType.BIGINT)
                .decode(getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value) {
        text.set(lexicoderMap.get(PrestoType.BIGINT).encode(value));
    }

    @Override
    public Time getTime(String name) {
        return new Time((Long) lexicoderMap.get(PrestoType.TIME)
                .decode(getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value) {
        text.set(lexicoderMap.get(PrestoType.TIME).encode(value.getTime()));
    }

    @Override
    public Timestamp getTimestamp(String name) {
        return new Timestamp((Long) lexicoderMap.get(PrestoType.TIMESTAMP)
                .decode(getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value) {
        text.set(
                lexicoderMap.get(PrestoType.TIMESTAMP).encode(value.getTime()));
    }

    @Override
    public byte[] getVarbinary(String name) {
        return (byte[]) lexicoderMap.get(PrestoType.VARBINARY)
                .decode(getFieldValue(name));
    }

    @Override
    public void setVarbinary(Text text, byte[] value) {
        text.set(lexicoderMap.get(PrestoType.VARBINARY).encode(value));
    }

    @Override
    public String getVarchar(String name) {
        return (String) lexicoderMap.get(PrestoType.VARCHAR)
                .decode(getFieldValue(name));
    }

    @Override
    public void setVarchar(Text text, String value) {
        text.set(lexicoderMap.get(PrestoType.VARCHAR).encode(value));
    }

    private byte[] getFieldValue(String name) {
        return columnValues.get(name);
    }
}
