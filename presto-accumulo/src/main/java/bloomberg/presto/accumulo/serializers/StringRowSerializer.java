package bloomberg.presto.accumulo.serializers;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Implementation of {@link StringRowSerializer} that encodes and decodes Presto column values as
 * human-readable String objects.
 */
public class StringRowSerializer
        implements AccumuloRowSerializer
{
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, Object> columnValues = new HashMap<>();
    private Text rowId = new Text();
    private Text cf = new Text();
    private Text cq = new Text();
    private Text value = new Text();
    private boolean rowOnly = false;
    private String rowIdName;

    @Override
    public void setRowIdName(String name)
    {
        this.rowIdName = name;
    }

    @Override
    public void setRowOnly(boolean rowOnly)
    {
        this.rowOnly = rowOnly;
    }

    @Override
    public void setMapping(String name, String fam, String qual)
    {
        columnValues.put(name, null);
        Map<String, String> q2pc = f2q2pc.get(fam);
        if (q2pc == null) {
            q2pc = new HashMap<>();
            f2q2pc.put(fam, q2pc);
        }

        q2pc.put(qual, name);
    }

    @Override
    public void reset()
    {
        columnValues.clear();
    }

    @Override
    public void deserialize(Entry<Key, Value> kvp)
            throws IOException
    {
        if (!columnValues.containsKey(rowIdName)) {
            kvp.getKey().getRow(rowId);
            columnValues.put(rowIdName, rowId.toString());
        }

        if (rowOnly) {
            return;
        }

        kvp.getKey().getColumnFamily(cf);
        kvp.getKey().getColumnQualifier(cq);
        value.set(kvp.getValue().get());
        columnValues.put(f2q2pc.get(cf.toString()).get(cq.toString()), value.toString());
    }

    @Override
    public boolean isNull(String name)
    {
        return columnValues.get(name) == null;
    }

    @Override
    public Block getArray(String name, Type type)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "arrays are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public void setArray(Text text, Type type, Block block)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "arrays are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public boolean getBoolean(String name)
    {
        return Boolean.parseBoolean(getFieldValue(name));
    }

    @Override
    public void setBoolean(Text text, Boolean value)
    {
        text.set(value.toString().getBytes());
    }

    @Override
    public Date getDate(String name)
    {
        return new Date(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setDate(Text text, Date value)
    {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public double getDouble(String name)
    {
        return Double.parseDouble(getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value)
    {
        text.set(value.toString().getBytes());
    }

    @Override
    public long getLong(String name)
    {
        return Long.parseLong(getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value)
    {
        text.set(value.toString().getBytes());
    }

    @Override
    public Block getMap(String name, Type type)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "maps are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public void setMap(Text text, Type type, Block block)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "maps are not (yet?) supported for StringRowSerializer");
    }

    @Override
    public Time getTime(String name)
    {
        return new Time(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value)
    {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public Timestamp getTimestamp(String name)
    {
        return new Timestamp(Long.parseLong(getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value)
    {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public byte[] getVarbinary(String name)
    {
        return getFieldValue(name).getBytes();
    }

    @Override
    public void setVarbinary(Text text, byte[] value)
    {
        text.set(value);
    }

    @Override
    public String getVarchar(String name)
    {
        return getFieldValue(name);
    }

    @Override
    public void setVarchar(Text text, String value)
    {
        text.set(value.getBytes());
    }

    private String getFieldValue(String name)
    {
        return columnValues.get(name).toString();
    }
}
