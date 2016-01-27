package bloomberg.presto.accumulo.iterators;

import com.google.common.primitives.UnsignedBytes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.Comparator;
import java.util.Iterator;

/**
 * A Combiner that does a lexicographic compare against values, returning the 'largest' value
 */
public class MaxByteArrayCombiner
        extends Combiner
{
    private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

    @Override
    public Value reduce(Key key, Iterator<Value> iter)
    {
        Value max = null;
        while (iter.hasNext()) {
            Value test = iter.next();
            if (max == null) {
                max = new Value(test.get());
            }
            else if (comparator.compare(test.get(), max.get()) > 0) {
                max.set(test.get());
            }
        }
        return max;
    }
}
