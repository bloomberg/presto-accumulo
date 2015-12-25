package bloomberg.presto.accumulo.serializers;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

import bloomberg.presto.accumulo.Types;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public interface AccumuloRowSerializer {

    public static AccumuloRowSerializer getDefault() {
        return new LexicoderRowSerializer();
    }

    public void setMapping(String name, String fam, String qual);

    public void deserialize(Entry<Key, Value> row) throws IOException;

    public boolean isNull(String name);

    public Block getArray(String name, Type type);

    public void setArray(Text text, Type type, Block block);

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

    public static Object getNativeContainerValue(Type type, Block block,
            int position) {
        if (block.isNull(position)) {
            return null;
        } else if (type.getJavaType() == boolean.class) {
            return type.getBoolean(block, position);
        } else if (type.getJavaType() == long.class) {
            return type.getLong(block, position);
        } else if (type.getJavaType() == double.class) {
            return type.getDouble(block, position);
        } else if (type.getJavaType() == Slice.class) {
            Slice slice = (Slice) type.getSlice(block, position);
            return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8()
                    : slice.getBytes();
        } else if (type.getJavaType() == Block.class) {
            return block;
        } else {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                    "Unimplemented type: " + type);
        }
    }

    @SuppressWarnings("rawtypes")
    public static List getArrayFromBlock(Type elementType, Block block) {
        List list = new ArrayList(block.getPositionCount());
        getArrayFromBlock(elementType, block, list);
        return list;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void getArrayFromBlock(Type elementType, Block block,
            List array) {
        if (Types.isArrayType(elementType)) {
            Type nestedElementType = Types.getElementType(elementType);
            for (int i = 0; i < block.getPositionCount(); ++i) {
                array.add(getArrayFromBlock(nestedElementType,
                        block.getObject(i, Block.class)));
            }
        } else {
            for (int i = 0; i < block.getPositionCount(); i++) {
                array.add(getNativeContainerValue(elementType, block, i));
            }
        }
    }

    public static Block getBlockFromArray(Type elementType, List<?> array) {
        BlockBuilder bldr = elementType
                .createBlockBuilder(new BlockBuilderStatus(), array.size());
        appendArrayElements(bldr, elementType, array);
        return bldr.build();
    }

    public static void appendArrayElements(BlockBuilder bldr, Type elementType,
            List<?> array) {
        for (Object o : array) {
            if (Types.isArrayType(elementType)) {
                BlockBuilder arrayBldr = bldr.beginBlockEntry();
                appendArrayElements(arrayBldr,
                        Types.getElementType(elementType), (List<?>) o);
                bldr.closeEntry();
            } else {
                switch (elementType.getDisplayName()) {
                case StandardTypes.BIGINT:
                    BigintType.BIGINT.writeLong(bldr, (Long) o);
                    break;
                case StandardTypes.BOOLEAN:
                    BooleanType.BOOLEAN.writeBoolean(bldr, (Boolean) o);
                    break;
                case StandardTypes.DATE:
                    DateType.DATE.writeLong(bldr, ((Date) o).getTime());
                    break;
                case StandardTypes.DOUBLE:
                    DoubleType.DOUBLE.writeDouble(bldr, (Double) o);
                    break;
                case StandardTypes.TIME:
                    TimeType.TIME.writeLong(bldr, ((Time) o).getTime());
                    break;
                case StandardTypes.TIMESTAMP:
                    TimestampType.TIMESTAMP.writeLong(bldr,
                            ((Timestamp) o).getTime());
                    break;
                case StandardTypes.VARBINARY:
                    VarbinaryType.VARBINARY.writeSlice(bldr,
                            Slices.wrappedBuffer((byte[]) o));
                    break;
                case StandardTypes.VARCHAR:
                    VarcharType.VARCHAR.writeSlice(bldr,
                            Slices.utf8Slice((String) o));
                    break;
                default:
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                            "Unsupported type " + elementType);
                }
            }
        }
    }
}
