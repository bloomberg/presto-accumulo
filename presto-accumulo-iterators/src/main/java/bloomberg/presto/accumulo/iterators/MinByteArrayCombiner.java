package bloomberg.presto.accumulo.iterators;

import com.google.common.primitives.UnsignedBytes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.Comparator;
import java.util.Iterator;

/**
 * A Combiner that does a lexicographic compare against values, returning the 'smallest' value
 */
public class MinByteArrayCombiner
        extends Combiner
{
    private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

    @Override
    public Value reduce(Key key, Iterator<Value> iter)
    {
        Value min = null;
        while (iter.hasNext()) {
            Value test = iter.next();
            if (min == null) {
                min = new Value(test.get());
            }
            else if (comparator.compare(test.get(), min.get()) < 0) {
                min.set(test.get());
            }
        }
        return min;
    }
}
