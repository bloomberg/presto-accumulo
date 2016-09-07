/**
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
package com.facebook.presto.accumulo.tools;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.metadata.AccumuloMetadataManager;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.DAY;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.HOUR;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.MINUTE;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision.SECOND;
import static com.facebook.presto.accumulo.index.metrics.MetricsStorage.getTruncatedTimestamps;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This task scans the index table of an Accumulo table, re-writing the metrics
 */
public class RewriteMetricsTask
        extends Task
{
    public static final String TASK_NAME = "rewritemetrics";
    public static final String DESCRIPTION = "Re-writes the metrics table based on the index table";

    private static final Logger LOG = Logger.get(RewriteMetricsTask.class);
    private static final Text CARDINALITY_CQ_AS_TEXT = new Text("___card___".getBytes(UTF_8));
    private static final Text METRICS_TABLE_ROW_ID_AS_TEXT = new Text("___METRICS_TABLE___".getBytes(UTF_8));
    private static final Text METRICS_TABLE_ROWS_COLUMN_AS_TEXT = new Text("___rows___".getBytes(UTF_8));

    private static final Map<TimestampPrecision, byte[]> TIMESTAMP_CARDINALITY_FAMILIES = ImmutableMap.of(
            SECOND, "_tss".getBytes(UTF_8),
            MINUTE, "_tsm".getBytes(UTF_8),
            HOUR, "_tsh".getBytes(UTF_8),
            DAY, "_tsd".getBytes(UTF_8));

    // Options
    private static final char SCHEMA_OPT = 's';
    private static final char TABLE_OPT = 't';
    private static final char AUTHORIZATIONS_OPT = 'a';

    // User-configured values
    private AccumuloConfig config;
    private Authorizations auths;
    private BatchWriterConfig bwc;
    private String schema;
    private String tableName;

    public int exec()
            throws Exception
    {
        // Validate the parameters have been set
        int numErrors = checkParam(config, "config");
        if (numErrors > 0) {
            return 1;
        }

        // Create the instance and the connector
        Instance inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        Connector connector = inst.getConnector(config.getUsername(), new PasswordToken(config.getPassword()));

        if (auths == null) {
            auths = connector.securityOperations().getUserAuthorizations(config.getUsername());
        }

        // Fetch the table metadata
        AccumuloMetadataManager manager = config.getMetadataManager(new TypeRegistry());

        LOG.info("Scanning Presto metadata for tables...");
        AccumuloTable table = manager.getTable(new SchemaTableName(schema, tableName));

        if (table == null) {
            LOG.error("Table is null, does it exist?");
            return 1;
        }

        long start = System.currentTimeMillis();

        reconfigureIterators(connector, table);

        ExecutorService service = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>()));

        List<Future<Void>> tasks = service.invokeAll(
                ImmutableList.of(() -> {
                    rewriteMetrics(connector, table, start);
                    return null;
                }, () -> {
                    rewriteNumRows(connector, table, start);
                    return null;
                }));

        for (Future<Void> task : tasks) {
            task.get();
        }

        LOG.info("Finished");
        return 0;
    }

    private void reconfigureIterators(Connector connector, AccumuloTable table)
            throws Exception
    {
        String tableName = table.getIndexTableName() + "_metrics";
        LOG.info("Reconfiguring iterators for " + tableName);

        IteratorSetting sumSetting = connector.tableOperations().getIteratorSetting(tableName, "SummingCombiner", IteratorScope.majc);
        sumSetting.setPriority(21);
        connector.tableOperations().removeIterator(tableName, sumSetting.getName(), EnumSet.allOf(IteratorScope.class));
        connector.tableOperations().attachIterator(tableName, sumSetting);

        IteratorSetting versSetting = connector.tableOperations().getIteratorSetting(tableName, "vers", IteratorScope.majc);
        VersioningIterator.setMaxVersions(versSetting, Integer.MAX_VALUE);
        connector.tableOperations().removeIterator(tableName, versSetting.getName(), EnumSet.allOf(IteratorScope.class));
        connector.tableOperations().attachIterator(tableName, versSetting);
    }

    private void rewriteMetrics(Connector connector, AccumuloTable table, long start)
    {
        LOG.info("Rewriting metrics for table " + table.getFullTableName());

        TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();
        BatchWriter writer = null;
        Scanner scanner = null;
        try {
            if (!dryRun) {
                LOG.info("Truncating metrics table " + table.getIndexTableName() + "_metrics");
                connector.tableOperations().deleteRows(table.getIndexTableName() + "_metrics", null, null);
            }
            else {
                LOG.info("Would have truncated metrics table " + table.getIndexTableName() + "_metrics");
            }

            writer = connector.createBatchWriter(table.getIndexTableName() + "_metrics", bwc);
            LOG.info("Created batch writer scanner against " + table.getIndexTableName() + "_metrics");

            scanner = new IsolatedScanner(connector.createScanner(table.getIndexTableName(), auths));
            LOG.info(format("Created isolated scanner against %s with auths %s", table.getIndexTableName(), auths));

            Set<Pair<String, String>> timestampColumns = table.isTruncateTimestamps()
                    ? table.getColumns().stream()
                    .filter(x -> x.getType().equals(TimestampType.TIMESTAMP) && x.getFamily().isPresent())
                    .map(x -> Pair.of(x.getFamily().get(), x.getQualifier().get()))
                    .collect(Collectors.toSet())
                    : ImmutableSet.of();

            LOG.info("Timestamp columns are " + timestampColumns);

            IteratorSetting timestampFilter = new IteratorSetting(21, "timestamp", TimestampFilter.class);
            TimestampFilter.setRange(timestampFilter, 0L, start);
            scanner.addScanIterator(timestampFilter);
            scanner.addScanIterator(new IteratorSetting(22, "wholerow", WholeRowIterator.class));
            Map<Text, Map<Text, Map<ColumnVisibility, AtomicLong>>> rowMap = new HashMap<>();
            long numMutations = 0L;
            boolean warned = true;
            for (Entry<Key, Value> entry : scanner) {
                Text key = null;
                for (Entry<Key, Value> row : WholeRowIterator.decodeRow(entry.getKey(), entry.getValue()).entrySet()) {
                    if (key == null) {
                        key = row.getKey().getRow();
                    }

                    Text family = row.getKey().getColumnFamily();
                    ColumnVisibility visibility = row.getKey().getColumnVisibilityParsed();

                    incrementMetric(rowMap, key, family, visibility);

                    String[] famQual = family.toString().split("_");

                    if (famQual.length == 2) {
                        if (timestampColumns.contains(Pair.of(famQual[0], famQual[1]))) {
                            incrementTimestampMetric(rowMap, family, visibility, row.getKey().getRow());
                        }
                    }
                    else if (warned) {
                        LOG.warn("Unable to re-write timestamp metric when either of a family/qualifier column mapping contains an underscore");
                        warned = false;
                    }
                }

                if (key != null) {
                    for (Entry<Text, Map<Text, Map<ColumnVisibility, AtomicLong>>> familyMap : rowMap.entrySet()) {
                        Mutation metricMutation = new Mutation(familyMap.getKey());
                        for (Entry<Text, Map<ColumnVisibility, AtomicLong>> familyEntry : familyMap.getValue().entrySet()) {
                            for (Entry<ColumnVisibility, AtomicLong> visibilityEntry : familyEntry.getValue().entrySet()) {
                                metricMutation.putDelete(
                                        familyEntry.getKey(),
                                        CARDINALITY_CQ_AS_TEXT,
                                        visibilityEntry.getKey(),
                                        start + 1);

                                if (visibilityEntry.getValue().get() > 0) {
                                    metricMutation.put(
                                            familyEntry.getKey(),
                                            CARDINALITY_CQ_AS_TEXT,
                                            visibilityEntry.getKey(),
                                            start + 2,
                                            new Value(encoder.encode(visibilityEntry.getValue().get())));
                                }
                            }
                        }
                        writer.addMutation(metricMutation);
                    }
                    ++numMutations;
                }

                rowMap.clear();
            }
            LOG.info("Finished rewriting metrics. Mutations written: " + numMutations);
        }
        catch (TableNotFoundException e) {
            LOG.error("Table not found, must have been deleted during process", e);
        }
        catch (IOException e) {
            LOG.error("Error decoding row", e);
        }
        catch (MutationsRejectedException e) {
            LOG.error("Server rejected mutations", e);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            LOG.error("Failed to truncate metrics table", e);
        }
        finally {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (MutationsRejectedException e) {
                    LOG.error("Server rejected mutations", e);
                }
            }

            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private void rewriteNumRows(Connector connector, AccumuloTable table, long start)
    {
        LOG.info("Rewriting number of rows metric for table " + table.getFullTableName());
        TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();
        BatchWriter writer = null;
        BatchScanner scanner = null;
        try {
            scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);
            LOG.info(format("Created batch scanner against %s with auths %s", table.getFullTableName(), auths));

            scanner.addScanIterator(new IteratorSetting(21, "firstentryiter", FirstEntryInRowIterator.class));
            IteratorSetting timestampFilter = new IteratorSetting(22, "timestamp", TimestampFilter.class);
            TimestampFilter.setRange(timestampFilter, 0L, start);
            scanner.addScanIterator(timestampFilter);
            scanner.setRanges(connector.tableOperations().splitRangeByTablets(table.getFullTableName(), new Range(), Integer.MAX_VALUE));

            Set<ColumnVisibility> visibilities = new HashSet<>();
            long sum = 0L;
            for (Entry<Key, Value> entry : scanner) {
                visibilities.add(entry.getKey().getColumnVisibilityParsed());
                ++sum;
            }

            writer = connector.createBatchWriter(table.getIndexTableName() + "_metrics", bwc);

            Mutation metricMutation = new Mutation(METRICS_TABLE_ROW_ID_AS_TEXT);
            for (ColumnVisibility visibility : visibilities) {
                metricMutation.putDelete(METRICS_TABLE_ROWS_COLUMN_AS_TEXT, CARDINALITY_CQ_AS_TEXT, visibility, start);
            }

            metricMutation.putDelete(METRICS_TABLE_ROWS_COLUMN_AS_TEXT, CARDINALITY_CQ_AS_TEXT, start);
            metricMutation.put(METRICS_TABLE_ROWS_COLUMN_AS_TEXT, CARDINALITY_CQ_AS_TEXT, start + 1, new Value(encoder.encode(sum)));

            writer.addMutation(metricMutation);
            LOG.info("Finished rewriting number of rows: " + sum);
        }
        catch (TableNotFoundException e) {
            LOG.error(format("Table %s not found, must have been deleted during process", table.getFullTableName()), e);
        }
        catch (MutationsRejectedException e) {
            LOG.error("Server rejected mutations", e);
        }
        catch (AccumuloSecurityException e) {
            LOG.error("Security exception getting Ranges", e);
        }
        catch (AccumuloException e) {
            LOG.error(e);
        }
        finally {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (MutationsRejectedException e) {
                    LOG.error("Server rejected mutations", e);
                }
            }

            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private void incrementMetric(Map<Text, Map<Text, Map<ColumnVisibility, AtomicLong>>> rowMap, Text row, Text family, ColumnVisibility visibility)
    {
        Map<Text, Map<ColumnVisibility, AtomicLong>> familyMap = rowMap.get(row);
        if (familyMap == null) {
            familyMap = new HashMap<>();
            rowMap.put(row, familyMap);
        }

        // Increment column cardinality
        Map<ColumnVisibility, AtomicLong> visibilityMap = familyMap.get(family);
        if (visibilityMap == null) {
            visibilityMap = new HashMap<>();
            visibilityMap.put(new ColumnVisibility(), new AtomicLong(0));
            familyMap.put(family, visibilityMap);
        }

        if (visibilityMap.containsKey(visibility)) {
            visibilityMap.get(visibility).incrementAndGet();
        }
        else {
            visibilityMap.put(visibility, new AtomicLong(1));
        }
    }

    private LexicoderRowSerializer serializer = new LexicoderRowSerializer();

    private void incrementTimestampMetric(Map<Text, Map<Text, Map<ColumnVisibility, AtomicLong>>> rowMap, Text family, ColumnVisibility visibility, Text timestampValue)
    {
        for (Entry<TimestampPrecision, Long> entry : getTruncatedTimestamps(serializer.decode(TIMESTAMP, timestampValue.copyBytes())).entrySet()) {
            Text timestampFamily = new Text(Bytes.concat(family.copyBytes(), TIMESTAMP_CARDINALITY_FAMILIES.get(entry.getKey())));

            Text row = new Text(serializer.encode(TIMESTAMP, entry.getValue()));
            Map<Text, Map<ColumnVisibility, AtomicLong>> familyMap = rowMap.get(row);
            if (familyMap == null) {
                familyMap = new HashMap<>();
                rowMap.put(row, familyMap);
            }

            Map<ColumnVisibility, AtomicLong> visibilityMap = familyMap.get(timestampFamily);
            if (visibilityMap == null) {
                visibilityMap = new HashMap<>();
                visibilityMap.put(new ColumnVisibility(), new AtomicLong(0));
                familyMap.put(timestampFamily, visibilityMap);
            }

            if (visibilityMap.containsKey(visibility)) {
                visibilityMap.get(visibility).incrementAndGet();
            }
            else {
                visibilityMap.put(visibility, new AtomicLong(1));
            }
        }
    }

    @Override
    public int run(AccumuloConfig config, CommandLine cmd)
            throws Exception
    {
        this.setConfig(config);
        if (cmd.hasOption(AUTHORIZATIONS_OPT)) {
            this.setAuthorizations(new Authorizations(cmd.getOptionValues(AUTHORIZATIONS_OPT)));
        }

        this.setSchema(cmd.getOptionValue(SCHEMA_OPT));
        this.setTableName(cmd.getOptionValue(TABLE_OPT));

        return this.exec();
    }

    public void setConfig(AccumuloConfig config)
    {
        this.config = config;
    }

    public void setAuthorizations(Authorizations auths)
    {
        this.auths = auths;
    }

    public void setBatchWriterConfig(BatchWriterConfig bwc)
    {
        this.bwc = bwc;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    @Override
    public String getTaskName()
    {
        return TASK_NAME;
    }

    @Override
    public String getDescription()
    {
        return DESCRIPTION;
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions()
    {
        Options opts = new Options();
        opts.addOption(
                OptionBuilder
                        .withLongOpt("authorizations")
                        .withDescription("List of scan authorizations.  Default is to get user authorizations for the user in the configuration.")
                        .hasArgs()
                        .create(AUTHORIZATIONS_OPT));
        opts.addOption(
                OptionBuilder
                        .withLongOpt("schema")
                        .withDescription("Presto schema name")
                        .hasArg()
                        .isRequired()
                        .create(SCHEMA_OPT));
        opts.addOption(
                OptionBuilder
                        .withLongOpt("table")
                        .withDescription("Presto table name")
                        .hasArg()
                        .isRequired()
                        .create(TABLE_OPT));
        return opts;
    }
}
