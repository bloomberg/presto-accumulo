package bloomberg.presto.accumulo.serializers;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
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
import com.facebook.presto.spi.type.Type;

import bloomberg.presto.accumulo.Types;
import io.airlift.slice.Slice;

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
    private boolean rowOnly = false;
    private String rowIdName;

    static {
        if (lexicoderMap == null) {
            lexicoderMap = new HashMap<>();
            lexicoderMap.put(BIGINT, new LongLexicoder());
            lexicoderMap.put(BOOLEAN, new BytesLexicoder());
            lexicoderMap.put(DATE, new LongLexicoder());
            lexicoderMap.put(DOUBLE, new DoubleLexicoder());
            lexicoderMap.put(TIME, new LongLexicoder());
            lexicoderMap.put(TIMESTAMP, new LongLexicoder());
            lexicoderMap.put(VARBINARY, new BytesLexicoder());
            lexicoderMap.put(VARCHAR, new StringLexicoder());
        }
    }

    @Override
    public void setRowIdName(String name) {
        rowIdName = name;
    }

    @Override
    public void setRowOnly(boolean rowOnly) {
        this.rowOnly = rowOnly;
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
        if (!columnValues.containsKey(rowIdName)) {
            kvp.getKey().getRow(rowId);
            columnValues.put(rowIdName, rowId.copyBytes());
        }

        if (rowOnly) {
            return;
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
                decode(type, getFieldValue(name)));
    }

    @Override
    public void setArray(Text text, Type type, Block block) {
        text.set(encode(type, block));
    }

    @Override
    public boolean getBoolean(String name) {
        return getFieldValue(name)[0] == TRUE[0];
    }

    @Override
    public void setBoolean(Text text, Boolean value) {
        text.set(encode(BOOLEAN, value));
    }

    @Override
    public Date getDate(String name) {
        return new Date(decode(BIGINT, getFieldValue(name)));
    }

    @Override
    public void setDate(Text text, Date value) {
        text.set(encode(DATE, value));
    }

    @Override
    public double getDouble(String name) {
        return decode(DOUBLE, getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value) {
        text.set(encode(DOUBLE, value));
    }

    @Override
    public long getLong(String name) {
        return decode(BIGINT, getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value) {
        text.set(encode(BIGINT, value));
    }

    @Override
    public Block getMap(String name, Type type) {
        return AccumuloRowSerializer.getBlockFromMap(type,
                decode(type, getFieldValue(name)));
    }

    @Override
    public void setMap(Text text, Type type, Block block) {
        text.set(encode(type, block));
    }

    @Override
    public Time getTime(String name) {
        return new Time(decode(BIGINT, getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value) {
        text.set(encode(TIME, value));
    }

    @Override
    public Timestamp getTimestamp(String name) {
        return new Timestamp(decode(TIMESTAMP, getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value) {
        text.set(encode(TIMESTAMP, value));
    }

    @Override
    public byte[] getVarbinary(String name) {
        return decode(VARBINARY, getFieldValue(name));
    }

    @Override
    public void setVarbinary(Text text, byte[] value) {
        text.set(encode(VARBINARY, value));
    }

    @Override
    public String getVarchar(String name) {
        return decode(VARCHAR, getFieldValue(name));
    }

    @Override
    public void setVarchar(Text text, String value) {
        text.set(encode(VARCHAR, value));
    }

    private byte[] getFieldValue(String name) {
        return columnValues.get(name);
    }

    /**
     * Encodes a Presto Java object to a byte array based on the given type.
     * 
     * Java Lists and Maps can be converted to Blocks using
     * {@link AccumuloRowSerializer#getBlockFromArray(Type, java.util.List)} and
     * {@link AccumuloRowSerializer#getBlockFromMap(Type, Map)}
     * 
     * Expected types are:<br>
     * <table>
     * <tr>
     * <th>Type to Encode</th>
     * <th>Expected Java Object</th>
     * </tr>
     * <tr>
     * <td>ARRAY</td>
     * <td>com.facebook.presto.spi.block.Block</td>
     * </tr>
     * <tr>
     * <td>BOOLEAN</td>
     * <td>Boolean</td>
     * </tr>
     * <tr>
     * <td>DATE</td>
     * <td>java.sql.Date, Long</td>
     * </tr>
     * <tr>
     * <td>DOUBLE</td>
     * <td>Double</td>
     * </tr>
     * <tr>
     * <td>LONG</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>Map</td>
     * <td>com.facebook.presto.spi.block.Block</td>
     * </tr>
     * <tr>
     * <td>Time</td>
     * <td>java.sql.Time, Long</td>
     * </tr>
     * <tr>
     * <td>Timestamp</td>
     * <td>java.sql.Timestamp, Long</td>
     * </tr>
     * <tr>
     * <td>VARBINARY</td>
     * <td>io.airlift.slice.Slice or byte[]</td>
     * </tr>
     * <tr>
     * <td>VARCHAR</td>
     * <td>io.airlift.slice.Slice or String</td>
     * </tr>
     * </table>
     * 
     * @param type
     *            The presto {@link com.facebook.presto.spi.type.Type}
     * @param v
     *            The Java object per the table in the method description
     * @return Encoded bytes
     */
    public static byte[] encode(Type type, Object v) {
        Object toEncode;
        if (Types.isArrayType(type)) {
            toEncode = AccumuloRowSerializer
                    .getArrayFromBlock(Types.getElementType(type), (Block) v);
        } else if (Types.isMapType(type)) {
            toEncode = AccumuloRowSerializer.getMapFromBlock(type, (Block) v);
        } else if (type.equals(BOOLEAN)) {
            toEncode = v.equals(Boolean.TRUE) ? LexicoderRowSerializer.TRUE
                    : LexicoderRowSerializer.FALSE;
        } else if (type.equals(DATE) && v instanceof Date) {
            toEncode = ((Date) v).getTime();
        } else if (type.equals(TIME) && v instanceof Time) {
            toEncode = ((Time) v).getTime();
        } else if (type.equals(TIMESTAMP) && v instanceof Timestamp) {
            toEncode = ((Timestamp) v).getTime();
        } else if (type.equals(VARBINARY) && v instanceof Slice) {
            toEncode = ((Slice) v).getBytes();
        } else if (type.equals(VARCHAR) && v instanceof Slice) {
            toEncode = ((Slice) v).toStringUtf8();
        } else {
            toEncode = v;
        }

        return getLexicoder(type).encode(toEncode);
    }

    /**
     * Generic function to decode the given byte array to a Java object based on
     * the given type.
     * 
     * Blocks from ARRAY and MAP types can be converted to Java Lists and Maps
     * using {@link AccumuloRowSerializer#getArrayFromBlock(Type, Block)} and
     * {@link AccumuloRowSerializer#getMapFromBlock(Type, Block)}
     * 
     * Expected types are:<br>
     * <table>
     * <tr>
     * <th>Encoded Type</th>
     * <th>Returned Java Object</th>
     * </tr>
     * <tr>
     * <td>ARRAY</td>
     * <td>List<?></td>
     * </tr>
     * <tr>
     * <td>BOOLEAN</td>
     * <td>Boolean</td>
     * </tr>
     * <tr>
     * <td>DATE</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>DOUBLE</td>
     * <td>Double</td>
     * </tr>
     * <tr>
     * <td>LONG</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>Map</td>
     * <td>Map<?,?></td>
     * </tr>
     * <tr>
     * <td>Time</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>Timestamp</td>
     * <td>Long</td>
     * </tr>
     * <tr>
     * <td>VARBINARY</td>
     * <td>byte[]</td>
     * </tr>
     * <tr>
     * <td>VARCHAR</td>
     * <td>String</td>
     * </tr>
     * </table>
     * 
     * @param type
     *            The presto {@link com.facebook.presto.spi.type.Type}
     * @param v
     *            Encoded bytes to decode
     * @return The Java object per the table in the method description
     */
    public static <T> T decode(Type type, byte[] v) {
        return (T) getLexicoder(type).decode(v);
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
