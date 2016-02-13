package bloomberg.presto.accumulo.index;

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ColumnCardinalityCache
{
    private final Authorizations auths;
    private static final Logger LOG = Logger.get(ColumnCardinalityCache.class);
    private final Connector conn;
    private final int size;
    private final int expireSeconds;

    private Map<String, TableColumnCache> tableToCache = new HashMap<>();

    public ColumnCardinalityCache(Connector conn, AccumuloConfig config, Authorizations auths)
            throws AccumuloException, AccumuloSecurityException
    {
        this.conn = conn;
        this.size = config.getCardinalityCacheSize();
        this.expireSeconds = config.getCardinalityCacheExpireSeconds();
        this.auths = auths;
    }

    public void deleteCache(String schema, String table)
    {
        if (tableToCache.containsKey(table)) {
            TableColumnCache tCache = getTableCache(schema, table);
            tCache.clear();
            tableToCache.remove(table);
        }
    }

    public List<Pair<AccumuloColumnConstraint, Long>> getCardinalities(String schema, String table, Map<AccumuloColumnConstraint, Collection<Range>> idxConstraintRangePairs)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ExecutionException
    {
        List<Pair<AccumuloColumnConstraint, Long>> retval = new ArrayList<>();
        for (Entry<AccumuloColumnConstraint, Collection<Range>> e : idxConstraintRangePairs.entrySet()) {
            long card = getColumnCardinality(schema, table, e.getKey(), e.getValue());
            LOG.debug("Cardinality for column %s is %d", e.getKey().getName(), card);
            retval.add(Pair.of(e.getKey(), card));
        }
        return retval;
    }

    public long getColumnCardinality(String schema, String table, AccumuloColumnConstraint acc, Collection<Range> indexRanges)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ExecutionException
    {
        return getTableCache(schema, table).getColumnCardinality(acc.getName(), acc.getFamily(), acc.getQualifier(), indexRanges);
    }

    private TableColumnCache getTableCache(String schema, String table)
    {
        TableColumnCache cache = tableToCache.get(AccumuloClient.getFullTableName(schema, table));
        if (cache == null) {
            cache = new TableColumnCache(schema, table);
            tableToCache.put(table, cache);
        }
        return cache;
    }

    private class TableColumnCache
    {
        private final Map<String, LoadingCache<Range, Long>> columnToCache = new HashMap<>();
        private final String schema;
        private final String table;

        public TableColumnCache(String schema, String table)
        {
            this.schema = schema;
            this.table = table;
        }

        public void clear()
        {
            for (LoadingCache<Range, Long> lc : columnToCache.values()) {
                lc.invalidateAll();
            }
            columnToCache.clear();
        }

        public long getColumnCardinality(String column, String family, String qualifier, Collection<Range> colValues)
                throws ExecutionException
        {
            LoadingCache<Range, Long> cache = columnToCache.get(column);
            if (cache == null) {
                cache = newCache(schema, table, family, qualifier);
                columnToCache.put(column, cache);
            }

            long sum = 0;
            for (Long e : cache.getAll(colValues).values()) {
                sum += e;
            }

            return sum;
        }

        private LoadingCache<Range, Long> newCache(String schema, String table, String family, String qualifier)
        {
            return CacheBuilder.newBuilder().maximumSize(size).expireAfterWrite(expireSeconds, TimeUnit.SECONDS).build(new AccumuloCacheLoader(schema, table, family, qualifier));
        }
    }

    private class AccumuloCacheLoader
            extends CacheLoader<Range, Long>
    {
        private final String metricsTable;
        private final Text columnFamily;

        private final Map<Range, Long> rangeValues = new MapDefaultZero();

        public AccumuloCacheLoader(String schema, String table, String family, String qualifier)
        {
            this.metricsTable = Indexer.getMetricsTableName(schema, table);
            columnFamily = new Text(Indexer.getIndexColumnFamily(family.getBytes(), qualifier.getBytes()).array());
        }

        @Override
        public Long load(Range key)
                throws Exception
        {
            Scanner scan = conn.createScanner(metricsTable, auths);
            scan.setRange(key);
            scan.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);

            long numEntries = 0;
            for (Entry<Key, Value> entry : scan) {
                numEntries += Long.parseLong(entry.getValue().toString());
            }
            scan.close();

            return numEntries;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<Range, Long> loadAll(Iterable<? extends Range> keys)
                throws Exception
        {
            LOG.debug("Scanning Accumulo for cardinality of %s ranges", ((Collection<Range>) keys).size());
            BatchScanner bScanner = conn.createBatchScanner(metricsTable, auths, 10);
            bScanner.setRanges((Collection<Range>) keys);
            bScanner.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);
            rangeValues.clear();

            for (Entry<Key, Value> entry : bScanner) {
                rangeValues.put(Range.exact(entry.getKey().getRow()), Long.parseLong(entry.getValue().toString()));
            }

            bScanner.close();
            return rangeValues;
        }

        /**
         * We extend HashMap here and override get to return a value of zero if the key is not in the map.
         * This mitigates the CacheLoader InvalidCacheLoadException if loadAll fails to return a value for a given
         * key, which occurs when there is no key in Accumulo.
         */
        public class MapDefaultZero
                extends HashMap<Range, Long>
        {
            private static final long serialVersionUID = -2511991250333716810L;

            @Override
            public Long get(Object key)
            {
                Long value = super.get(key);
                return value == null ? 0 : value;
            }
        }
    }
}
