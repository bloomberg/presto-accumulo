package bloomberg.presto.accumulo.iterators;

import java.io.IOException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;

public class AndFilter extends AbstractBooleanFilter {

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator)
            throws IOException {
        for (RowFilter f : filters) {
            if (!f.acceptRow(rowIterator)) {
                return false;
            }
        }

        return true;
    }

    public static IteratorSetting andFilters(int priority,
            IteratorSetting... configs) {
        return combineFilters(OrFilter.class, priority, configs);
    }
}