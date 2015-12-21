package bloomberg.presto.accumulo.serializers;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.iterators.ValueFormatException;

class BooleanLexicoder implements Lexicoder<Boolean> {

    @Override
    public byte[] encode(Boolean v) {
        return v ? new byte[] { 1 } : new byte[] { 0 };
    }

    @Override
    public Boolean decode(byte[] b) throws ValueFormatException {
        return b[0] == 0 ? false : true;
    }
}