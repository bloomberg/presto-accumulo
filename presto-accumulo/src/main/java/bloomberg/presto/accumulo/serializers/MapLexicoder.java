package bloomberg.presto.accumulo.serializers;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.concat;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.escape;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.split;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.unescape;

public class MapLexicoder<K, V>
        implements Lexicoder<Map<K, V>>
{
    private Lexicoder<K> keyLexicoder;
    private Lexicoder<V> valueLexicoder;

    public MapLexicoder(Lexicoder<K> keyLexicoder, Lexicoder<V> valueLexicoder)
    {
        this.keyLexicoder = keyLexicoder;
        this.valueLexicoder = valueLexicoder;
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
