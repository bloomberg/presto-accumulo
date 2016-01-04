package bloomberg.presto.accumulo.serializers;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ListLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

import bloomberg.presto.accumulo.Types;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class LexicoderRowSerializer implements AccumuloRowSerializer {
    public static final byte[] TRUE = new byte[] { 1 };
    public static final byte[] FALSE = new byte[] { 0 };
    private static Map<Type, Lexicoder> lexicoderMap = null;
    private static Map<String, ListLexicoder<?>> listLexicoders = new HashMap<>();
    private static Map<String, MapLexicoder<?, ?>> mapLexicoders = new HashMap<>();
    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, byte[]> columnValues = new HashMap<>();
    private Text rowId = new Text(), cf = new Text(), cq = new Text(),
            value = new Text();

    static {
        if (lexicoderMap == null) {
            lexicoderMap = new HashMap<>();
            lexicoderMap.put(BigintType.BIGINT, new LongLexicoder());
            lexicoderMap.put(BooleanType.BOOLEAN, new BytesLexicoder());
            lexicoderMap.put(DateType.DATE, new LongLexicoder());
            lexicoderMap.put(DoubleType.DOUBLE, new DoubleLexicoder());
            lexicoderMap.put(TimeType.TIME, new LongLexicoder());
            lexicoderMap.put(TimestampType.TIMESTAMP, new LongLexicoder());
            lexicoderMap.put(VarbinaryType.VARBINARY, new BytesLexicoder());
            lexicoderMap.put(VarcharType.VARCHAR, new StringLexicoder());
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
    }

    @Override
    public void reset() {
        columnValues.clear();
    }

    @Override
    public void deserialize(Entry<Key, Value> kvp) throws IOException {
        if (!columnValues
                .containsKey(AccumuloMetadataManager.ROW_ID_COLUMN_NAME)) {
            kvp.getKey().getRow(rowId);
            columnValues.put(AccumuloMetadataManager.ROW_ID_COLUMN_NAME,
                    rowId.copyBytes());
        }

        kvp.getKey().getColumnFamily(cf);
        kvp.getKey().getColumnQualifier(cq);
        value.set(kvp.getValue().get());
        columnValues.put(f2q2pc.get(cf.toString()).get(cq.toString()),
                value.copyBytes());
    }

    @Override
    public boolean isNull(String name) {
        return columnValues.get(name) == null;
    }

    @Override
    public Block getArray(String name, Type type) {
        Type elementType = Types.getElementType(type);
        return AccumuloRowSerializer.getBlockFromArray(elementType,
                getListLexicoder(type).decode(getFieldValue(name)));
    }

    @Override
    public void setArray(Text text, Type type, Block block) {
        Type elementType = Types.getElementType(type);

        List array = AccumuloRowSerializer.getArrayFromBlock(elementType,
                block);

        text.set(getListLexicoder(type).encode(array));
    }

    @Override
    public boolean getBoolean(String name) {
        return getFieldValue(name)[0] == TRUE[0];
    }

    @Override
    public void setBoolean(Text text, Boolean value) {
        text.set(
                getLexicoder(BooleanType.BOOLEAN).encode(value ? TRUE : FALSE));
    }

    @Override
    public Date getDate(String name) {
        return new Date((Long) (getLexicoder(BigintType.BIGINT)
                .decode(getFieldValue(name))));
    }

    @Override
    public void setDate(Text text, Date value) {
        text.set(getLexicoder(BigintType.BIGINT).encode(value.getTime()));
    }

    @Override
    public double getDouble(String name) {
        return (Double) getLexicoder(DoubleType.DOUBLE)
                .decode(getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value) {
        text.set(getLexicoder(DoubleType.DOUBLE).encode(value));
    }

    @Override
    public long getLong(String name) {
        return (Long) getLexicoder(BigintType.BIGINT)
                .decode(getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value) {
        text.set(getLexicoder(BigintType.BIGINT).encode(value));
    }

    @Override
    public Block getMap(String name, Type type) {
        return AccumuloRowSerializer.getBlockFromMap(type,
                getMapLexicoder(type).decode(getFieldValue(name)));
    }

    @Override
    public void setMap(Text text, Type type, Block block) {
        text.set(getMapLexicoder(type)
                .encode(AccumuloRowSerializer.getMapFromBlock(type, block)));
    }

    @Override
    public Time getTime(String name) {
        return new Time((Long) getLexicoder(BigintType.BIGINT)
                .decode(getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value) {
        text.set(getLexicoder(BigintType.BIGINT).encode(value.getTime()));
    }

    @Override
    public Timestamp getTimestamp(String name) {
        return new Timestamp((Long) getLexicoder(BigintType.BIGINT)
                .decode(getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value) {
        text.set(getLexicoder(BigintType.BIGINT).encode(value.getTime()));
    }

    @Override
    public byte[] getVarbinary(String name) {
        return (byte[]) getLexicoder(VarbinaryType.VARBINARY)
                .decode(getFieldValue(name));
    }

    @Override
    public void setVarbinary(Text text, byte[] value) {
        text.set(getLexicoder(VarbinaryType.VARBINARY).encode(value));
    }

    @Override
    public String getVarchar(String name) {
        return (String) getLexicoder(VarcharType.VARCHAR)
                .decode(getFieldValue(name));
    }

    @Override
    public void setVarchar(Text text, String value) {
        text.set(getLexicoder(VarcharType.VARCHAR).encode(value));
    }

    private byte[] getFieldValue(String name) {
        return columnValues.get(name);
    }

    public static Lexicoder getLexicoder(Type type) {
        if (Types.isArrayType(type)) {
            return getListLexicoder(type);
        } else if (Types.isMapType(type)) {
            return getMapLexicoder(type);
        } else {
            Lexicoder l = (Lexicoder) lexicoderMap.get(type);
            if (l == null) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "No lexicoder for type " + type);
            }
            return l;
        }
    }

    private static ListLexicoder getListLexicoder(Type type) {
        ListLexicoder<?> listLexicoder = listLexicoders
                .get(type.getDisplayName());
        if (listLexicoder == null) {
            listLexicoder = new ListLexicoder(
                    getLexicoder(Types.getElementType(type)));
            listLexicoders.put(type.getDisplayName(), listLexicoder);
        }
        return listLexicoder;
    }

    private static MapLexicoder getMapLexicoder(Type type) {
        MapLexicoder<?, ?> mapLexicoder = mapLexicoders
                .get(type.getDisplayName());
        if (mapLexicoder == null) {
            mapLexicoder = new MapLexicoder(
                    getLexicoder(Types.getKeyType(type)),
                    getLexicoder(Types.getValueType(type)));
            mapLexicoders.put(type.getDisplayName(), mapLexicoder);
        }
        return mapLexicoder;
    }
}
