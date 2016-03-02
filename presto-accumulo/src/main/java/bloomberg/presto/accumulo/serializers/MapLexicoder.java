package bloomberg.presto.accumulo.serializers;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.concat;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.escape;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.split;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.unescape;

/**
 * Accumulo lexicoder for encoding a Java Map
 *
 * @param <K>
 *            Key data type
 * @param <V>
 *            Value data type
 */
public class MapLexicoder<K, V>
        implements Lexicoder<Map<K, V>>
{
    private Lexicoder<K> keyLexicoder;
    private Lexicoder<V> valueLexicoder;

    /**
     * Creates a new {@link MapLexicoder} using the given lexicoders for key and value data types
     *
     * @param kl
     *            Lexicoder for the keys
     * @param vl
     *            Lexicoder for the values
     */
    public MapLexicoder(Lexicoder<K> kl, Lexicoder<V> vl)
    {
        this.keyLexicoder = kl;
        this.valueLexicoder = vl;
    }

    @Override
    public byte[] encode(Map<K, V> v)
    {
        byte[][] elements = new byte[v.size() * 2][];
        int index = 0;
        for (Entry<K, V> e : v.entrySet()) {
            elements[index++] = escape(keyLexicoder.encode(e.getKey()));
            elements[index++] = escape(valueLexicoder.encode(e.getValue()));
        }

        return concat(elements);
    }

    @Override
    public Map<K, V> decode(byte[] b)
    {
        byte[][] escapedElements = split(b);
        Map<K, V> ret = new HashMap<K, V>();

        for (int i = 0; i < escapedElements.length; i += 2) {
            K key = keyLexicoder.decode(unescape(escapedElements[i]));
            V value = valueLexicoder.decode(unescape(escapedElements[i + 1]));
            ret.put(key, value);
        }

        return ret;
    }
}
