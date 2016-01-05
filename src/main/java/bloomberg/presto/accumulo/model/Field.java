package bloomberg.presto.accumulo.model;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;

import bloomberg.presto.accumulo.Types;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import io.airlift.slice.Slice;

public class Field {
    private Object value;
    private Type type;

    public Field(Object v, Type t) {
        this.value = Field.cleanObject(v, t);
        this.type = t;
    }

    public Field(Field f) {
        this.type = f.type;

        if (Types.isArrayType(this.type) || Types.isMapType(this.type)) {
            this.value = f.value;
            return;
        }

        switch (type.getDisplayName()) {
        case StandardTypes.BIGINT:
            this.value = new Long(f.getBigInt());
            break;
        case StandardTypes.BOOLEAN:
            this.value = new Boolean(f.getBoolean());
            break;
        case StandardTypes.DATE:
            this.value = new Date(f.getDate().getTime());
            break;
        case StandardTypes.DOUBLE:
            this.value = new Double(f.getDouble());
            break;
        case StandardTypes.TIME:
            this.value = new Time(f.getTime().getTime());
            break;
        case StandardTypes.TIMESTAMP:
            this.value = new Timestamp(f.getTimestamp().getTime());
            break;
        case StandardTypes.VARBINARY:
            this.value = Arrays.copyOf(f.getVarbinary(),
                    f.getVarbinary().length);
            break;
        case StandardTypes.VARCHAR:
            this.value = new String(f.getVarchar());
            break;
        default:
            throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    public Type getType() {
        return type;
    }

    public Block getArray() {
        return (Block) value;
    }

    public Long getBigInt() {
        return (Long) value;
    }

    public Block getBlock() {
        return (Block) value;
    }

    public Boolean getBoolean() {
        return (Boolean) value;
    }

    public Date getDate() {
        return (Date) value;
    }

    public void setDate(long value) {
        ((Date) this.value).setTime(value);
    }

    public Double getDouble() {
        return (Double) value;
    }

    public Object getIntervalDatToSecond() {
        throw new UnsupportedOperationException();
    }

    public Object getIntervalYearToMonth() {
        throw new UnsupportedOperationException();
    }

    public Block getMap() {
        return (Block) value;
    }

    public Object getObject() {
        return value;
    }

    public Timestamp getTimestamp() {
        return (Timestamp) value;
    }

    public Object getTimestampWithTimeZone() {
        throw new UnsupportedOperationException();
    }

    public Time getTime() {
        return (Time) value;
    }

    public Object getTimeWithTimeZone() {
        throw new UnsupportedOperationException();
    }

    public byte[] getVarbinary() {
        return (byte[]) value;
    }

    public String getVarchar() {
        return (String) value;
    }

    public boolean isNull() {
        return value == null;
    }

    public static Object cleanObject(Object v, Type t) {
        if (v == null) {
            return v;
        }

        if (Types.isArrayType(t)) {
            if (!(v instanceof Block))
                throw new RuntimeException(
                        "Object is not a Block, but " + v.getClass());
            return v;
        }

        if (Types.isMapType(t)) {
            if (!(v instanceof Block))
                throw new RuntimeException(
                        "Object is not a Block, but " + v.getClass());
            return v;
        }

        // Validate the object is the given type
        switch (t.getDisplayName()) {

        case StandardTypes.BIGINT:
            // Auto-convert integers to Longs
            if (v instanceof Integer)
                return new Long((Integer) v);
            if (!(v instanceof Long))
                throw new RuntimeException(
                        "Object is not a Long, but " + v.getClass());
            break;
        case StandardTypes.BOOLEAN:
            if (!(v instanceof Boolean))
                throw new RuntimeException(
                        "Object is not a Boolean, but " + v.getClass());
            return new Boolean((boolean) v);
        case StandardTypes.DATE:
            if (v instanceof Long)
                return new Date((Long) v);

            if (!(v instanceof Date))
                throw new RuntimeException(
                        "Object is not a Date, but " + v.getClass());
            break;
        case StandardTypes.DOUBLE:
            if (!(v instanceof Double))
                throw new RuntimeException(
                        "Object is not a Double, but " + v.getClass());
            break;
        case StandardTypes.TIME:
            if (v instanceof Long)
                return new Time((Long) v);

            if (!(v instanceof Time))
                throw new RuntimeException(
                        "Object is not a Time, but " + v.getClass());
            break;
        case StandardTypes.TIMESTAMP:
            if (v instanceof Long)
                return new Timestamp((Long) v);

            if (!(v instanceof Timestamp))
                throw new RuntimeException(
                        "Object is not a Timestamp, but " + v.getClass());
            break;
        case StandardTypes.VARBINARY:
            if (v instanceof Slice) {
                return ((Slice) v).getBytes();
            }

            if (!(v instanceof byte[]))
                throw new RuntimeException(
                        "Object is not a byte[], but " + v.getClass());
            break;
        case StandardTypes.VARCHAR:
            if (v instanceof Slice) {
                return new String(((Slice) v).getBytes());
            }

            if (!(v instanceof String))
                throw new RuntimeException(
                        "Object is not a String, but " + v.getClass());
            break;
        default:
            throw new RuntimeException("Unsupported PrestoType " + t);
        }

        return v;
    }

    @Override
    public boolean equals(Object obj) {
        boolean retval = true;
        if (obj instanceof Field) {
            Field f = (Field) obj;
            if (type.equals(f.getType())) {
                if (type.equals(VarbinaryType.VARBINARY)) {
                    // special case for byte arrays
                    // aren't they so fancy
                    retval = Arrays.equals((byte[]) value,
                            (byte[]) f.getObject());
                } else if (type.equals(DateType.DATE)
                        || type.equals(TimeType.TIME)
                        || type.equals(TimestampType.TIMESTAMP)) {
                    retval = value.toString().equals(f.getObject().toString());
                } else {
                    if (value instanceof Block) {
                        retval = equals((Block) value, (Block) f.getObject());
                    } else {
                        retval = value.equals(f.getObject());
                    }
                }
            }
        }
        return retval;
    }

    private boolean equals(Block b1, Block b2) {
        boolean retval = b1.getPositionCount() == b2.getPositionCount();
        for (int i = 0; i < b1.getPositionCount() && retval; ++i) {
            if (b1 instanceof ArrayBlock && b2 instanceof ArrayBlock) {
                retval = equals(b1.getObject(i, Block.class),
                        b2.getObject(i, Block.class));
            } else {
                retval = b1.compareTo(i, 0, b1.getLength(i), b2, i, 0,
                        b2.getLength(i)) == 0;
            }
        }
        return retval;
    }

    @Override
    public String toString() {
        if (value == null) {
            return "null";
        }

        if (Types.isArrayType(type)) {
            Type et = Types.getElementType(type);
            StringBuilder bldr = new StringBuilder("ARRAY [");
            for (Object f : AccumuloRowSerializer.getArrayFromBlock(et,
                    this.getArray())) {
                if (Types.isArrayType(et)) {
                    Type eet = Types.getElementType(et);
                    bldr.append(new Field(AccumuloRowSerializer
                            .getBlockFromArray(eet, (List<?>) f), et))
                            .append(',');
                } else if (Types.isMapType(et)) {
                    bldr.append(new Field(AccumuloRowSerializer
                            .getBlockFromMap(et, (Map<?, ?>) f), et))
                            .append(',');
                } else {
                    bldr.append(new Field(f, et)).append(',');
                }
            }

            return bldr.deleteCharAt(bldr.length() - 1).append("]").toString();
        }

        if (Types.isMapType(type)) {
            StringBuilder bldr = new StringBuilder("MAP(");
            StringBuilder keys = new StringBuilder("ARRAY [");
            StringBuilder values = new StringBuilder("ARRAY [");
            for (Entry<Object, Object> e : AccumuloRowSerializer
                    .getMapFromBlock(type, this.getMap()).entrySet()) {

                Type kt = Types.getKeyType(type);
                if (Types.isArrayType(kt)) {
                    keys.append(new Field(AccumuloRowSerializer
                            .getBlockFromArray(Types.getElementType(kt),
                                    (List<?>) e.getKey()),
                            kt)).append(',');
                } else if (Types.isMapType(kt)) {
                    keys.append(new Field(AccumuloRowSerializer
                            .getBlockFromMap(kt, (Map<?, ?>) e.getKey()), kt))
                            .append(',');
                } else {
                    keys.append(new Field(e.getKey(), kt)).append(',');
                }

                Type vt = Types.getValueType(type);
                if (Types.isArrayType(vt)) {
                    values.append(new Field(AccumuloRowSerializer
                            .getBlockFromArray(Types.getElementType(vt),
                                    (List<?>) e.getValue()),
                            vt)).append(',');
                } else if (Types.isMapType(vt)) {
                    values.append(new Field(AccumuloRowSerializer
                            .getBlockFromMap(vt, (Map<?, ?>) e.getValue()), vt))
                            .append(',');
                } else {
                    values.append(new Field(e.getValue(), vt)).append(',');
                }
            }

            keys.deleteCharAt(keys.length() - 1).append(']');
            values.deleteCharAt(values.length() - 1).append(']');
            return bldr.append(keys).append(", ").append(values).append(")")
                    .toString();
        }

        // Validate the object is the given type
        switch (type.getDisplayName()) {
        case StandardTypes.BIGINT:
        case StandardTypes.BOOLEAN:
        case StandardTypes.DOUBLE:
            return value.toString();
        case StandardTypes.DATE:
            return "DATE '" + ((Date) value).toString() + "'";
        case StandardTypes.TIME:
            return "TIME '" + ((Time) value).toString() + "'";
        case StandardTypes.TIMESTAMP:
            return "TIMESTAMP '" + ((Timestamp) value).toString() + "'";
        case StandardTypes.VARBINARY:
            return "CAST('" + new String((byte[]) value) + "' AS VARBINARY)";
        case StandardTypes.VARCHAR:
            return "'" + value.toString().replaceAll("'", "''") + "'";
        default:
            throw new RuntimeException("Unsupported PrestoType " + type);
        }
    }
}
