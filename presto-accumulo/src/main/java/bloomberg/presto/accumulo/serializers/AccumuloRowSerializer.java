package bloomberg.presto.accumulo.serializers;

import bloomberg.presto.accumulo.Types;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Interface for deserializing the data in Accumulo into a Presto row.<br>
 * <br>
 * Provides a means for end-users of the connector to customize how the data in an Accumulo row gets
 * serialized and deserialized from/to a Presto row.<br>
 * <br>
 * The workflow of how this class is called by the Accumulo connector for reading data is as
 * follows:
 * <ol>
 * <li>setRowIdName - Sets the Presto name which is the Accumulo row ID</li>
 * <li>setRowOnly - True if only the row ID is going to be retrieved, false if more data is
 * necessary.</li>
 * <li>setMapping - Multiple calls for each Presto column, setting the mapping of Presto column name
 * to Accumulo column family and qualifier</li>
 * <li>deserialize - Called for each Accumulo entry in the same row. Implements should
 * retrieve the Presto column value from the given key/value pair</li>
 * <li>get* - Called to retrieve the data type for the given Presto column name</li>
 * <li>reset - Begins a new Row, serializer is expected to clear any state</li>
 * <li>If there are more entries left, go back to deserialize, else end!</li>
 * </ol>
 *
 * @see LexicoderRowSerializer
 * @see StringRowSerializer
 */
public interface AccumuloRowSerializer
{
    /**
     * Gets the default AccumuloRowSerializer.
     *
     * @return Default serializer
     */
    public static AccumuloRowSerializer getDefault()
    {
        return new LexicoderRowSerializer();
    }

    /**
     * Sets the Presto name which maps to the Accumulo row ID
     *
     * @param name
     *            Presto column name
     */
    public void setRowIdName(String name);

    /**
     * Sets the mapping for the Presto column name to Accumulo family and qualifier
     *
     * @param name
     *            Presto name
     * @param fam
     *            Accumulo family
     * @param qual
     *            Accumulo qualifier
     */
    public void setMapping(String name, String fam, String qual);

    /**
     * Sets a Boolean value indicating whether or not only the row ID is going to be retrieved from
     * the serializer
     *
     * @param rowOnly
     *            True if only the row ID is set, false otherwise
     */
    public void setRowOnly(boolean rowOnly);

    /**
     * Reset the state of the serializer to prepare for a new set of entries with the same row ID
     */
    public void reset();

    /**
     * Deserialize the given Accumulo entry, retrieving data for the Presto column
     *
     * @param kvp
     *            Entry to deserialize
     * @throws IOException
     *             If an IO error occurs during deserialization
     */
    public void deserialize(Entry<Key, Value> kvp)
            throws IOException;

    /**
     * Gets a Boolean value indicating whether or not the Presto column is a null value
     *
     * @param name
     *            Column name
     * @return True if null, false otherwise.
     */
    public boolean isNull(String name);

    /**
     * Gets the array Block of the given Presto column
     *
     * @param name
     *            Column name
     * @param type
     *            Array type
     * @return True if null, false otherwise.
     */
    public Block getArray(String name, Type type);

    /**
     * Encode the given array Block into the given Text object
     *
     * @param text
     *            Text object to set
     * @param type
     *            Array type
     * @param block
     *            Array block
     */
    public void setArray(Text text, Type type, Block block);

    /**
     * Gets the Boolean value of the given Presto column
     *
     * @param name
     *            Column name
     * @return Boolean value
     */
    public boolean getBoolean(String name);

    /**
     * Encode the given Boolean value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setBoolean(Text text, Boolean value);

    /**
     * Gets the Date value of the given Presto column
     *
     * @param name
     *            Column name
     * @return Date value
     */
    public Date getDate(String name);

    /**
     * Encode the given Date value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setDate(Text text, Date value);

    /**
     * Gets the Double value of the given Presto column
     *
     * @param name
     *            Column name
     * @return Double value
     */
    public double getDouble(String name);

    /**
     * Encode the given Double value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setDouble(Text text, Double value);

    /**
     * Gets the Long value of the given Presto column
     *
     * @param name
     *            Column name
     * @return Long value
     */
    public long getLong(String name);

    /**
     * Encode the given Long value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setLong(Text text, Long value);

    /**
     * Gets the Map value of the given Presto column and Map type
     *
     * @param name
     *            Column name
     * @param type
     *            Map type
     * @return Map value
     */
    public Block getMap(String name, Type type);

    /**
     * Encode the given map Block into the given Text object
     *
     * @param text
     *            Text object to set
     * @param type
     *            Map type
     * @param block
     *            Map block
     */
    public void setMap(Text text, Type type, Block block);

    /**
     * Gets the Time value of the given Presto column
     *
     * @param name
     *            Column name
     * @return Time value
     */
    public Time getTime(String name);

    /**
     * Encode the given Time value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setTime(Text text, Time value);

    /**
     * Gets the Timestamp value of the given Presto column
     *
     * @param name
     *            Column name
     * @return Timestamp value
     */
    public Timestamp getTimestamp(String name);

    /**
     * Encode the given Timestamp value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setTimestamp(Text text, Timestamp value);

    /**
     * Gets the Varbinary value of the given Presto column
     *
     * @param name
     *            Column name
     * @return Varbinary value
     */
    public byte[] getVarbinary(String name);

    /**
     * Encode the given byte[] value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setVarbinary(Text text, byte[] value);

    /**
     * Gets the String value of the given Presto column
     *
     * @param name
     *            Column name
     * @return String value
     */
    public String getVarchar(String name);

    /**
     * Encode the given String value into the given Text object
     *
     * @param text
     *            Text object to set
     * @param value
     *            Value to encode
     */
    public void setVarchar(Text text, String value);

    /**
     * Given the array element type and Presto Block, decodes the Block into a list of values.
     *
     * @param elementType
     *            Array element type
     * @param block
     *            Array block
     * @return List of values
     */
    public static List<Object> getArrayFromBlock(Type elementType, Block block)
    {
        List<Object> array = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); ++i) {
            array.add(readObject(elementType, block, i));
        }
        return array;
    }

    /**
     * Given the map type and Presto Block, decodes the Block into a map of values.
     *
     * @param type
     *            Map type
     * @param block
     *            Map block
     * @return List of values
     */
    public static Map<Object, Object> getMapFromBlock(Type type, Block block)
    {
        Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
        Type kt = Types.getKeyType(type);
        Type vt = Types.getValueType(type);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put(readObject(kt, block, i), readObject(vt, block, i + 1));
        }
        return map;
    }

    /**
     * Encodes the given list into a Block.
     *
     * @param elementType
     *            Element type of the array
     * @param array
     *            Array of elements to encode
     * @return Presto Block
     */
    public static Block getBlockFromArray(Type elementType, List<?> array)
    {
        BlockBuilder bldr = elementType.createBlockBuilder(new BlockBuilderStatus(), array.size());
        for (Object item : (List<?>) array) {
            writeObject(bldr, elementType, item);
        }
        return bldr.build();
    }

    /**
     * Encodes the given map into a Block.
     *
     * @param mapType
     *            Presto type of the map
     * @param map
     *            Map of key/value pairs to encode
     * @return Presto Block
     */
    public static Block getBlockFromMap(Type mapType, Map<?, ?> map)
    {
        Type keyType = mapType.getTypeParameters().get(0);
        Type valueType = mapType.getTypeParameters().get(1);

        BlockBuilder bldr = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType),
                new BlockBuilderStatus(), map.size() * 2);

        for (Entry<?, ?> entry : map.entrySet()) {
            writeObject(bldr, keyType, entry.getKey());
            writeObject(bldr, valueType, entry.getValue());
        }
        return bldr.build();
    }

    /**
     * Recursive helper function used by {@link AccumuloRowSerializer#getBlockFromArray} and
     * {@link AccumuloRowSerializer#getBlockFromMap} to add the given object to the given block
     * builder. Supports nested complex types!
     *
     * @param bldr
     *            Block builder
     * @param type
     *            Presto type
     * @param o
     *            Object to write to the block builder
     */
    static void writeObject(BlockBuilder bldr, Type type, Object o)
    {
        if (Types.isArrayType(type)) {
            BlockBuilder arrayBldr = bldr.beginBlockEntry();
            Type et = Types.getElementType(type);
            for (Object item : (List<?>) o) {
                writeObject(arrayBldr, et, item);
            }
            bldr.closeEntry();
        }
        else if (Types.isMapType(type)) {
            Type kt = ((MapType) type).getKeyType();
            Type vt = ((MapType) type).getValueType();
            BlockBuilder mapBlockBuilder = bldr.beginBlockEntry();
            for (Entry<?, ?> entry : ((Map<?, ?>) o).entrySet()) {
                writeObject(mapBlockBuilder, kt, entry.getKey());
                writeObject(mapBlockBuilder, vt, entry.getValue());
            }
            bldr.closeEntry();
        }
        else {
            TypeUtils.writeNativeValue(type, bldr, o);
        }
    }

    /**
     * Recursive helper function used by {@link AccumuloRowSerializer#getArrayFromBlock} and
     * {@link AccumuloRowSerializer#getMapFromBlock} to decode the Block into a Java type
     *
     * @param type
     *            Presto type
     * @param block
     *            Block to decode
     * @param position
     *            Position in the block to get
     * @return Java object from the Block
     */
    static Object readObject(Type type, Block block, int position)
    {
        if (Types.isArrayType(type)) {
            Type elementType = Types.getElementType(type);
            return getArrayFromBlock(elementType, block.getObject(position, Block.class));
        }
        else if (Types.isMapType(type)) {
            return getMapFromBlock(type, block.getObject(position, Block.class));
        }
        else {
            if (type.getJavaType() == Slice.class) {
                Slice slice = (Slice) TypeUtils.readNativeValue(type, block, position);
                return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8() : slice.getBytes();
            }
            else {
                return TypeUtils.readNativeValue(type, block, position);
            }
        }
    }
}
