package bloomberg.presto.accumulo;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Text;

public class StringRowSerializer implements AccumuloRowSerializer {

    @Override
    public void setBoolean(Text text, Boolean value) {
        text.set(value.toString().getBytes());
    }

    @Override
    public void setDate(Text text, Date value) {
        text.set(Long.toString(TimeUnit.MILLISECONDS.toDays(value.getTime()))
                .getBytes());
    }

    @Override
    public void setDouble(Text text, Double value) {
        text.set(value.toString().getBytes());
    }

    @Override
    public void setLong(Text text, Long value) {
        text.set(value.toString().getBytes());
    }

    @Override
    public void setTime(Text text, Time value) {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public void setTimestamp(Text text, Timestamp value) {
        text.set(Long.toString(value.getTime()).getBytes());
    }

    @Override
    public void setVarbinary(Text text, byte[] value) {
        text.set(value);
    }

    @Override
    public void setVarchar(Text text, String value) {
        text.set(value.getBytes());
    }
}
