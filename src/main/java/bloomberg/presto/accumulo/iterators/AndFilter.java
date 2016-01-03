package bloomberg.presto.accumulo.iterators;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public class AndFilter extends AbstractBooleanFilter {

    @Override
    public boolean accept(Key k, Value v) {
        for (Filter f : filters) {
            if (!f.accept(k, v)) {
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