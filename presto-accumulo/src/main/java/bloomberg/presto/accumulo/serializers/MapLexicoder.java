package bloomberg.presto.accumulo.serializers;

import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.concat;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.escape;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.split;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.unescape;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;

public class MapLexicoder<KT, VT> implements Lexicoder<Map<KT, VT>> {

    private Lexicoder<KT> keyLexicoder;
    private Lexicoder<VT> valueLexicoder;

    public MapLexicoder(Lexicoder<KT> keyLexicoder,
            Lexicoder<VT> valueLexicoder) {
        this.keyLexicoder = keyLexicoder;
        this.valueLexicoder = valueLexicoder;
    }

    @Override
    public byte[] encode(Map<KT, VT> v) {
        byte[][] elements = new byte[v.size() * 2][];
        int index = 0;
        for (Entry<KT, VT> e : v.entrySet()) {
            elements[index++] = escape(keyLexicoder.encode(e.getKey()));
            elements[index++] = escape(valueLexicoder.encode(e.getValue()));
        }
        return concat(elements);
    }

    @Override
    public Map<KT, VT> decode(byte[] b) {
        byte[][] escapedElements = split(b);
        Map<KT, VT> ret = new TreeMap<KT, VT>();

        for (int i = 0; i < escapedElements.length; i += 2) {
            KT key = keyLexicoder.decode(unescape(escapedElements[i]));
            VT value = valueLexicoder.decode(unescape(escapedElements[i + 1]));
            ret.put(key, value);
        }

        return ret;
    }
}
