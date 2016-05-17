/*
 * Copyright 2016 Bloomberg L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.index;

import com.facebook.presto.accumulo.AccumuloErrorCode;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This class is an indexing utility to cache the cardinality of a column value for every table.
 * Each table has its own cache that is independent of every other, and every column also has its
 * own Guava cache. Use of this utility can have a significant impact for retrieving the cardinality
 * of many columns, preventing unnecessary accesses to the metrics table in Accumulo for a
 * cardinality that won't change much.
 */
public class ColumnCardinalityCache
{
    private final Authorizations auths;
    private static final Logger LOG = Logger.get(ColumnCardinalityCache.class);
    private final Connector conn;
    private final int size;
    private final Duration expireDuration;
    private final ExecutorService executorService;
    private Map<String, TableColumnCache> tableToCache = new HashMap<>();

    /**
     * Creates a new instance of {@link ColumnCardinalityCache}
     *
     * @param conn Accumulo connector
     * @param config Connector configuration for presto
     * @param auths Authorizations to access Accumulo
     */
    public ColumnCardinalityCache(Connector conn, AccumuloConfig config, Authorizations auths)
    {
        this.conn = requireNonNull(conn, "conn is null");
        this.size = requireNonNull(config, "config is null").getCardinalityCacheSize();
        this.expireDuration = config.getCardinalityCacheExpiration();
        this.auths = requireNonNull(auths, "auths is null");

        // Create executor service with one hot thread, pool size capped at 4x processors,
        // one minute keep alive, and a labeled ThreadFactory
        AtomicLong threadCount = new AtomicLong(0);
        this.executorService = MoreExecutors.getExitingExecutorService(
                new ThreadPoolExecutor(1, 4 * Runtime.getRuntime().availableProcessors(), 60L,
                        TimeUnit.SECONDS, new SynchronousQueue<>(), runnable ->
                        new Thread(runnable, "cardinality-lookup-thread-" + threadCount.getAndIncrement())
                ));
    }

    /**
     * Deletes any cache for the given table, no-op of table does not exist in the cache
     *
     * @param schema Schema name
     * @param table Table name
     */
    public void deleteCache(String schema, String table)
    {
        LOG.debug("Deleting cache for %s.%s", schema, table);
        if (tableToCache.containsKey(table)) {
            // clear the cache and remove it
            getTableCache(schema, table).clear();
            tableToCache.remove(table);
        }
    }

    /**
     * Gets the cardinality for each {@link AccumuloColumnConstraint}. Given constraints are
     * expected to be indexed! Who knows what would happen if they weren't!
     *
     * @param schema Schema name
     * @param table Table name
     * @param idxConstraintRangePairs Mapping of all ranges for a given constraint
     * @param earlyReturnThreshold Smallest acceptable cardinality to return early while other tasks complete
     * @return An immutable multimap of cardinality to column constraint, sorted by cardinality from smallest to largest
     * @throws AccumuloException If an error occurs retrieving the cardinalities from Accumulo
     * @throws AccumuloSecurityException If a security exception is raised
     * @throws TableNotFoundException If the metrics table does not exist
     * @throws ExecutionException If another error occurs; I really don't even know anymore.
     */
    public Multimap<Long, AccumuloColumnConstraint> getCardinalities(String schema, String table,
            Multimap<AccumuloColumnConstraint, Range> idxConstraintRangePairs, long earlyReturnThreshold)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
            ExecutionException
    {
        // Create a multi map sorted by cardinality
        ListMultimap<Long, AccumuloColumnConstraint> cardinalityToConstraints =
                Multimaps.synchronizedListMultimap(MultimapBuilder.treeKeys().arrayListValues().build());

        // Submit tasks to the executor to fetch column cardinality, adding it to the Guava cache if necessary
        CompletionService<Long> executor = new ExecutorCompletionService<>(executorService);
        idxConstraintRangePairs.asMap().entrySet().stream().forEach(e ->
                executor.submit(() -> {
                            long cardinality = getColumnCardinality(schema, table, e.getKey(), e.getValue());
                            LOG.info("Cardinality for column %s is %d", e.getKey().getName(), cardinality);
                            cardinalityToConstraints.put(cardinality, e.getKey());
                            return cardinality;
                        }
                ));

        try {
            int numTasks = idxConstraintRangePairs.asMap().entrySet().size();
            for (int i = 0; i < numTasks; ++i) {
                Long columnCardinality = executor.take().get();
                if (columnCardinality <= earlyReturnThreshold) {
                    LOG.info("Cardinality %d, is below threshold. Returning early while other tasks finish",
                            columnCardinality);
                    break;
                }
            }
        }
        catch (ExecutionException | InterruptedException e) {
            throw new PrestoException(AccumuloErrorCode.INTERNAL_ERROR, "Exception when getting cardinality", e);
        }

        // Create a copy of the cardinalities
        return ImmutableMultimap.copyOf(cardinalityToConstraints);
    }

    /**
     * Gets the cardinality for the given column constraint with the given Ranges. Ranges can be
     * exact values or a range of values
     *
     * @param schema Schema name
     * @param table Table name
     * @param acc Mapping of all ranges for a given constraint
     * @param indexRanges Ranges for each exact or ranged value of the column constraint
     * @return A list of
     * @throws AccumuloException If an error occurs retrieving the cardinalities from Accumulo
     * @throws AccumuloSecurityException If a security exception is raised
     * @throws TableNotFoundException If the metrics table does not exist
     * @throws ExecutionException If another error occurs; I really don't even know anymore.
     */
    private long getColumnCardinality(String schema, String table, AccumuloColumnConstraint acc,
            Collection<Range> indexRanges)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
            ExecutionException
    {
        return getTableCache(schema, table).getColumnCardinality(acc.getName(), acc.getFamily(),
                acc.getQualifier(), indexRanges);
    }

    /**
     * Gets the {@link TableColumnCache} for the given table, creating a new one if necessary.
     *
     * @param schema Schema name
     * @param table Table name
     * @return An existing or new TableColumnCache
     */
    private TableColumnCache getTableCache(String schema, String table)
    {
        String fullName = AccumuloTable.getFullTableName(schema, table);
        TableColumnCache cache = tableToCache.get(fullName);
        if (cache == null) {
            LOG.debug("Creating new TableColumnCache for %s.%s %s", schema, table, this);
            cache = new TableColumnCache(schema, table);
            tableToCache.put(fullName, cache);
        }
        return cache;
    }

    /**
     * Internal class for holding the mapping of column names to the LoadingCache
     */
    private class TableColumnCache
    {
        private final Map<String, LoadingCache<Range, Long>> columnToCache = new HashMap<>();
        private final String schema;
        private final String table;

        /**
         * Creates a new instance of {@link TableColumnCache}
         *
         * @param schema Schema name
         * @param table Table name
         */
        public TableColumnCache(String schema, String table)
        {
            this.schema = schema;
            this.table = table;
        }

        /**
         * Clears and removes all caches as if the object had been first created
         */
        public void clear()
        {
            for (LoadingCache<Range, Long> lc : columnToCache.values()) {
                lc.invalidateAll();
            }
            columnToCache.clear();
        }

        /**
         * Gets the column cardinality for all of the given range values. May reach out to the
         * metrics table in Accumulo to retrieve new cache elements.
         *
         * @param column Presto column name
         * @param family Accumulo column family
         * @param qualifier Accumulo column qualifier
         * @param colValues All range values to summarize for the cardinality
         * @return The cardinality of the column
         * @throws ExecutionException
         * @throws TableNotFoundException
         */
        public long getColumnCardinality(String column, String family, String qualifier,
                Collection<Range> colValues)
                throws ExecutionException, TableNotFoundException
        {
            LOG.debug("Getting cardinality for %s %s %s %s", column, family, qualifier, colValues);
            // Get the column cache for this column, creating a new one if necessary
            LoadingCache<Range, Long> cache = columnToCache.get(column);
            if (cache == null) {
                cache = newCache(schema, table, family, qualifier);
                columnToCache.put(column, cache);
            }

            // Collect all exact Accumulo Ranges, i.e. single value entries vs. a full scan
            Collection<Range> exactRanges =
                    colValues.stream().filter(this::isExact).collect(Collectors.toList());
            LOG.debug("Column values contain %s exact ranges of %s", exactRanges.size(),
                    colValues.size());

            // Sum the cardinalities for the exact-value Ranges
            // This is where the reach-out to Accumulo occurs for all Ranges that have not
            // previously been fetched
            long sum = 0;
            for (Long e : cache.getAll(exactRanges).values()) {
                sum += e;
            }

            // If these collection sizes are not equal,
            // then there is at least one non-exact range
            if (exactRanges.size() != colValues.size()) {
                // for each range in the column value
                for (Range r : colValues) {
                    // if this range is not exact
                    if (!isExact(r)) {
                        // Then get the value for this range using the single-value cache lookup
                        long val = cache.get(r);

                        // add our value to the cache and our sum
                        cache.put(r, val);
                        sum += val;
                    }
                }
            }

            LOG.debug("Cache stats : size=%s, %s", cache.size(), cache.stats());
            return sum;
        }

        /**
         * Gets a Boolean value indicating if the given Range is an exact value
         *
         * @param r Range to check
         * @return True if exact, false otherwise
         */
        private boolean isExact(Range r)
        {
            return !r.isInfiniteStartKey() && !r.isInfiniteStopKey()
                    && r.getStartKey().followingKey(PartialKey.ROW).equals(r.getEndKey());
        }

        /**
         * Creates a new cache for the given column
         *
         * @param schema Schema name
         * @param table Table name
         * @param family Accumulo column family for the column
         * @param qualifier Accumulo qualifier for the column
         * @return A fresh LoadingCache
         */
        private LoadingCache<Range, Long> newCache(String schema, String table, String family,
                String qualifier)
        {
            LOG.debug("Created new cache for %s.%s, column %s:%s, size %d expiry %s", schema, table,
                    family, qualifier, size, expireDuration);
            return CacheBuilder.newBuilder().maximumSize(size)
                    .expireAfterWrite(expireDuration.toMillis(), TimeUnit.MILLISECONDS)
                    .build(new CardinalityCacheLoader(schema, table, family, qualifier));
        }
    }

    /**
     * Internal class for loading the cardinality from Accumulo
     */
    private class CardinalityCacheLoader
            extends CacheLoader<Range, Long>
    {
        private final String metricsTable;
        private final Text columnFamily;

        /**
         * Creates a new instance of {@link CardinalityCacheLoader}
         *
         * @param schema Schema name
         * @param table Table name
         * @param family Accumulo family for the Presto column
         * @param qualifier Accumulo qualifier for the Presto column
         */
        public CardinalityCacheLoader(String schema, String table, String family, String qualifier)
        {
            this.metricsTable = Indexer.getMetricsTableName(schema, table);

            // Create the column family for our scanners
            this.columnFamily = new Text(
                    Indexer.getIndexColumnFamily(family.getBytes(), qualifier.getBytes()).array());
        }

        /**
         * Loads the cardinality for the given Range. Uses a Scanner and sums the cardinality for
         * all values that encapsulate the Range.
         *
         * @param key Range to get the cardinality for
         * @return The cardinality of the column, which would be zero if the value does not exist
         */
        @Override
        public Long load(Range key)
                throws Exception
        {
            LOG.debug("Loading a non-exact range from Accumulo: %s", key);
            // Create batch scanner for querying all ranges
            Scanner bScanner = conn.createScanner(metricsTable, auths);
            try {
                bScanner.setRange(key);
                bScanner.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);

                // Sum the entries to get the cardinality
                long numEntries = 0;
                for (Entry<Key, Value> entry : bScanner) {
                    numEntries += Long.parseLong(entry.getValue().toString());
                }
                return numEntries;
            }
            finally {
                if (bScanner != null) {
                    // Don't forget to close your scanner before returning the cardinalities
                    bScanner.close();
                }
            }
        }

        /**
         * Loads the cardinality for a collection of Range objects
         *
         * @param keys All keys to load
         * @return A mapping of Range to cardinality
         */
        @Override
        public Map<Range, Long> loadAll(Iterable<? extends Range> keys)
                throws Exception
        {
            @SuppressWarnings("unchecked")
            Collection<Range> rangeKeys = (Collection<Range>) keys;
            LOG.debug("Loading %s exact ranges from Accumulo", rangeKeys.size());

            // Create batch scanner for querying all ranges
            BatchScanner bScanner = conn.createBatchScanner(metricsTable, auths, 10);
            try {
                bScanner.setRanges(rangeKeys);
                bScanner.fetchColumn(columnFamily, Indexer.CARDINALITY_CQ_AS_TEXT);

                // Create a new map to hold our cardinalities for each range, returning a default of
                // Zero for each non-existent Key
                Map<Range, Long> rangeValues = new MapDefaultZero();
                for (Entry<Key, Value> entry : bScanner) {
                    rangeValues.put(Range.exact(entry.getKey().getRow()),
                            Long.parseLong(entry.getValue().toString()));
                }
                return rangeValues;
            }
            finally {
                if (bScanner != null) {
                    // Don't forget to close your scanner before returning the cardinalities
                    bScanner.close();
                }
            }
        }

        /**
         * We extend HashMap here and override get to return a value of zero if the key is not in
         * the map. This mitigates the CacheLoader InvalidCacheLoadException if loadAll fails to
         * return a value for a given key, which occurs when there is no key in Accumulo.
         */
        public class MapDefaultZero
                extends HashMap<Range, Long>
        {
            private static final long serialVersionUID = -2511991250333716810L;

            /**
             * Gets the value associated with the given key, or zero if the key is not found
             */
            @Override
            public Long get(Object key)
            {
                // Get the key from our map overlord
                Long value = super.get(key);

                // Return zero if null
                return value == null ? 0 : value;
            }
        }
    }
}
