package bloomberg.presto.accumulo.serializers;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public interface AccumuloRowSerializer {

    public static AccumuloRowSerializer getDefault() {
        return new StringRowSerializer();
    }

    public void setMapping(String name, String fam, String qual);

    public void deserialize(Entry<Key, Value> row) throws IOException;

    public boolean isNull(String name);

    public boolean getBoolean(String name);

    public void setBoolean(Text text, Boolean value);

    public Date getDate(String name);

    public void setDate(Text text, Date value);

    public double getDouble(String name);

    public void setDouble(Text text, Double value);

    public long getLong(String name);

    public void setLong(Text text, Long value);

    public Time getTime(String name);

    public void setTime(Text text, Time value);

    public Timestamp getTimestamp(String name);

    public void setTimestamp(Text text, Timestamp value);

    public byte[] getVarbinary(String name);

    public void setVarbinary(Text text, byte[] value);

    public String getVarchar(String name);

    public void setVarchar(Text text, String value);
}
