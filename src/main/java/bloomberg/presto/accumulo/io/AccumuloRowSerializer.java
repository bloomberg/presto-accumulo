package bloomberg.presto.accumulo.io;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.io.Text;

public interface AccumuloRowSerializer {

    public static AccumuloRowSerializer getDefault() {
        return new StringRowSerializer();
    }

    public void setBoolean(Text text, Boolean value);

    public void setDate(Text text, Date value);

    public void setDouble(Text text, Double value);

    public void setLong(Text text, Long value);

    public void setTime(Text text, Time value);

    public void setTimestamp(Text text, Timestamp value);

    public void setVarbinary(Text text, byte[] value);

    public void setVarchar(Text text, String value);
}
