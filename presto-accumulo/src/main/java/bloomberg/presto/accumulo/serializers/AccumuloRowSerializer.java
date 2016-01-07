package bloomberg.presto.accumulo.serializers;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;

import bloomberg.presto.accumulo.Types;
import io.airlift.slice.Slice;

public interface AccumuloRowSerializer {

    public static AccumuloRowSerializer getDefault() {
        return new LexicoderRowSerializer();
    }

    public void setMapping(String name, String fam, String qual);

    public void reset();

    public void deserialize(Entry<Key, Value> kvp) throws IOException;

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

    public Block getMap(String name, Type type);

    public void setMap(Text text, Type type, Block block);

    public void setLong(Text text, Long value);

    public Time getTime(String name);

    public void setTime(Text text, Time value);

    public Timestamp getTimestamp(String name);

    public void setTimestamp(Text text, Timestamp value);

    public byte[] getVarbinary(String name);

    public void setVarbinary(Text text, byte[] value);

    public String getVarchar(String name);

    public void setVarchar(Text text, String value);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static List getArrayFromBlock(Type elementType, Block block) {
        List array = new ArrayList(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); ++i) {
            array.add(readObject(elementType, block, i));
        }
        return array;
    }

    public static Map<Object, Object> getMapFromBlock(Type type, Block block) {
        Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
        Type kt = Types.getKeyType(type);
        Type vt = Types.getValueType(type);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put(readObject(kt, block, i), readObject(vt, block, i + 1));
        }
        return map;
    }

    public static Block getBlockFromArray(Type elementType, List<?> array) {
        BlockBuilder bldr = elementType
                .createBlockBuilder(new BlockBuilderStatus(), array.size());
        for (Object item : (List<?>) array) {
            writeObject(bldr, elementType, item);
        }
        return bldr.build();
    }

    public static Block getBlockFromMap(Type mapType, Map<?, ?> map) {
        Type keyType = mapType.getTypeParameters().get(0);
        Type valueType = mapType.getTypeParameters().get(1);

        BlockBuilder bldr = new InterleavedBlockBuilder(
                ImmutableList.of(keyType, valueType), new BlockBuilderStatus(),
                map.size() * 2);

        for (Entry<?, ?> entry : map.entrySet()) {
            writeObject(bldr, keyType, entry.getKey());
            writeObject(bldr, valueType, entry.getValue());
        }
        return bldr.build();
    }

    public static void writeObject(BlockBuilder bldr, Type elementType,
            Object o) {
        if (Types.isArrayType(elementType)) {
            BlockBuilder arrayBldr = bldr.beginBlockEntry();
            Type itemType = Types.getElementType(elementType);
            for (Object item : (List<?>) o) {
                writeObject(arrayBldr, itemType, item);
            }
            bldr.closeEntry();
        } else if (Types.isMapType(elementType)) {
            Type kt = ((MapType) elementType).getKeyType();
            Type vt = ((MapType) elementType).getValueType();
            BlockBuilder mapBlockBuilder = bldr.beginBlockEntry();
            for (Entry<?, ?> entry : ((Map<?, ?>) o).entrySet()) {
                writeObject(mapBlockBuilder, kt, entry.getKey());
                writeObject(mapBlockBuilder, vt, entry.getValue());
            }
            bldr.closeEntry();
        } else {
            TypeUtils.writeNativeValue(elementType, bldr, o);
        }
    }

    public static Object readObject(Type type, Block block, int position) {
        if (Types.isArrayType(type)) {
            Type elementType = Types.getElementType(type);
            return getArrayFromBlock(elementType,
                    block.getObject(position, Block.class));
        } else if (Types.isMapType(type)) {
            return getMapFromBlock(type,
                    block.getObject(position, Block.class));
        } else {
            if (type.getJavaType() == Slice.class) {
                Slice slice = (Slice) TypeUtils.readNativeValue(type, block,
                        position);
                return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8()
                        : slice.getBytes();
            } else {
                return TypeUtils.readNativeValue(type, block, position);
            }
        }
    }
}
