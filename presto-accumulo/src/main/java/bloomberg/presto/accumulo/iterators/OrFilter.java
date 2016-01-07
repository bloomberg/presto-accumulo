package bloomberg.presto.accumulo.iterators;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;

public class OrFilter extends AbstractBooleanFilter {

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator)
            throws IOException {
        for (RowFilter f : filters) {
            if (f.acceptRow(rowIterator)) {
                return true;
            }
            rowIterator.seek(new Range(), new HashSet<>(), false);
        }

        return false;
    }

    public static IteratorSetting orFilters(int priority,
            IteratorSetting... configs) {
        return combineFilters(OrFilter.class, priority, configs);
    }

    public static IteratorSetting orFilters(int priority,
            List<IteratorSetting> configs) {
        return combineFilters(OrFilter.class, priority,
                configs.toArray(new IteratorSetting[0]));
    }
}