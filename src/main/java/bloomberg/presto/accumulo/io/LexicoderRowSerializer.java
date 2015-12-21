package bloomberg.presto.accumulo.io;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.hadoop.io.Text;

import bloomberg.presto.accumulo.PrestoType;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class LexicoderRowSerializer implements AccumuloRowSerializer {
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
    public void setBoolean(Text text, Boolean value) {
        text.set(lexicoderMap.get(PrestoType.BOOLEAN).encode(value));
    }

    @Override
    public void setDate(Text text, Date value) {
        text.set(lexicoderMap.get(PrestoType.DATE)
                .encode(TimeUnit.MILLISECONDS.toDays(value.getTime())));
    }

    @Override
    public void setDouble(Text text, Double value) {
        text.set(lexicoderMap.get(PrestoType.DOUBLE).encode(value));
    }

    @Override
    public void setLong(Text text, Long value) {
        text.set(lexicoderMap.get(PrestoType.BIGINT).encode(value));
    }

    @Override
    public void setTime(Text text, Time value) {
        text.set(lexicoderMap.get(PrestoType.TIME).encode(value.getTime()));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value) {
        text.set(
                lexicoderMap.get(PrestoType.TIMESTAMP).encode(value.getTime()));
    }

    @Override
    public void setVarbinary(Text text, byte[] value) {
        text.set(lexicoderMap.get(PrestoType.VARBINARY).encode(value));
    }

    @Override
    public void setVarchar(Text text, String value) {
        text.set(lexicoderMap.get(PrestoType.VARCHAR).encode(value));
    }
}
