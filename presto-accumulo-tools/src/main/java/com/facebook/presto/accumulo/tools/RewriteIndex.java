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
package com.facebook.presto.accumulo.tools;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.index.metrics.MetricsStorage.TimestampPrecision;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.iterators.ColumnTimestampFilter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.IndexColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.index.Indexer.HYPHEN_BYTE;
import static com.facebook.presto.accumulo.index.Indexer.TIMESTAMP_CARDINALITY_FAMILIES;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This task scans the index table of a  table, re-writing the index,
 * then invoking the RewriteMetricsTask to finalize the task.
 */
public class RewriteIndex
        extends Task
{
    public static final String TASK_NAME = "rewriteindex";
    public static final String DESCRIPTION = "Re-writes the index and metrics table based on the data table";

    private static final Logger LOG = Logger.getLogger(RewriteIndex.class);

    // Options
    private static final char SCHEMA_OPT = 's';
    private static final char TABLE_OPT = 't';
    private static final char AUTHORIZATIONS_OPT = 'a';
    private static final String FORCE_OPT = "force";
    private static final String ADD_ONLY_OPT = "add-only";
    private static final String COLUMNS_OPT = "l";

    // User-configured values
    private AccumuloConfig config;
    private Authorizations auths;
    private BatchWriterConfig bwc;
    private String schema;
    private String tableName;
    private boolean dryRun;
    private boolean addOnly;
    private Optional<List<String>> columns = Optional.empty();

    private long numDeletedIndexEntries = 0L;
    private boolean incrementRowMetric = true;

    public int exec()
            throws Exception
    {
        // Validate the required parameters have been set
        int numErrors = checkParam(config, "config");
        numErrors += checkParam(schema, "schema");
        numErrors += checkParam(tableName, "tableName");
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
        ZooKeeperMetadataManager manager = new ZooKeeperMetadataManager(config, new TypeRegistry());

        LOG.info("Scanning Presto metadata for tables...");
        AccumuloTable table = manager.getTable(new SchemaTableName(schema, tableName));

        if (table == null) {
            LOG.error("Table is null, does it exist?");
            return 1;
        }

        long start = System.currentTimeMillis();

        truncateMetricsTable(connector, table, start);

        addIndexEntries(connector, table, start);

        if (!addOnly) {
            deleteIndexEntries(connector, table, start);
        }
        else {
            LOG.info("Add only is true, only added index entries.  Did not delete index or rewrite metrics.");
        }

        LOG.info("Finished re-writing index.");
        return 0;
    }

    private void truncateMetricsTable(Connector connector, AccumuloTable table, long start)
            throws AccumuloSecurityException, AccumuloException, TableNotFoundException
    {
        String metricsTable = table.getIndexTableName() + "_metrics";

        if (!dryRun) {
            if (columns.isPresent() && columns.get().size() > 0) {
                LOG.info("Selecting columns: " + columns.get());
                List<String> settingNames = new ArrayList<>();
                int numIterators = 0;
                NEXT_COLUMN:
                for (IndexColumn indexColumn : table.getParsedIndexColumns()) {
                    Text family = new Text();
                    AccumuloColumnHandle handle = null;
                    for (String column : indexColumn.getColumns()) {
                        if (!columns.get().contains(column)) {
                            continue NEXT_COLUMN;
                        }

                        if (family.getLength() != 0) {
                            family.append(HYPHEN_BYTE, 0, HYPHEN_BYTE.length);
                        }
                        handle = table.getColumn(column);
                        byte[] bytes = (handle.getFamily().get() + "_" + handle.getQualifier().get()).getBytes(UTF_8);
                        family.append(bytes, 0, bytes.length);
                    }

                    settingNames.add(family.toString());

                    IteratorSetting setting = new IteratorSetting(MAX_VALUE - numIterators++, family.toString(), ColumnTimestampFilter.class);
                    ColumnTimestampFilter.addTimestamp(setting, new IteratorSetting.Column(family, null), start);

                    LOG.info(format("Attaching iterator to drop %s from table %s ", family, metricsTable));
                    connector.tableOperations().attachIterator(metricsTable, setting);

                    if (handle != null && handle.getType().equals(TIMESTAMP) && table.isTruncateTimestamps()) {
                        for (Entry<TimestampPrecision, byte[]> entry : TIMESTAMP_CARDINALITY_FAMILIES.entrySet()) {
                            if (entry.getValue().length == 0) {
                                continue;
                            }

                            Text timestampFamily = new Text(family);
                            timestampFamily.append(entry.getValue(), 0, entry.getValue().length);

                            String timestampName = family.toString() + new String(entry.getValue(), UTF_8);

                            settingNames.add(timestampName);

                            setting = new IteratorSetting(MAX_VALUE - numIterators++, timestampName, ColumnTimestampFilter.class);
                            ColumnTimestampFilter.addTimestamp(setting, new IteratorSetting.Column(timestampFamily, null), start);

                            LOG.info(format("Attaching iterator to drop %s from table %s ", timestampFamily, metricsTable));
                            connector.tableOperations().attachIterator(metricsTable, setting);
                        }
                    }
                }

                LOG.info("Truncating select columns from table " + metricsTable + " via compaction");
                connector.tableOperations().compact(metricsTable, new CompactionConfig());

                for (String family : settingNames) {
                    LOG.info(format("Detaching iterator to drop %s from table %s ", family, metricsTable));
                    connector.tableOperations().removeIterator(metricsTable, family, EnumSet.allOf(IteratorUtil.IteratorScope.class));
                }

                incrementRowMetric = false;
            }
            else {
                LOG.info("Truncating table " + metricsTable);
                connector.tableOperations().deleteRows(metricsTable, null, null);
            }
        }
        else {
            LOG.info("Would have truncated metrics table " + metricsTable);
        }
    }

    private void addIndexEntries(Connector connector, AccumuloTable table, long start)
    {
        LOG.info(format("Scanning data table %s to add index entries", table.getFullTableName()));
        Scanner scanner = null;
        BatchWriter indexWriter = null;
        MetricsWriter metricsWriter = table.getMetricsStorageInstance(connector).newWriter(table);
        try {
            // Create index writer and metrics writer, but we are never going to flush the metrics writer
            indexWriter = connector.createBatchWriter(table.getIndexTableName(), bwc);
            Indexer indexer = new Indexer(connector, table, indexWriter, metricsWriter);
            LOG.info("Created indexer against " + table.getIndexTableName());

            scanner = connector.createScanner(table.getFullTableName(), auths);
            LOG.info(format("Created scanner against %s with auths %s", table.getFullTableName(), auths));

            setScannerColumns(table, scanner);

            IteratorSetting timestampFilter = new IteratorSetting(21, "timestamp", TimestampFilter.class);
            TimestampFilter.setRange(timestampFilter, 0L, start);
            scanner.addScanIterator(timestampFilter);

            long numRows = 0L;
            Text prevRow = null;
            Text row = new Text();
            Text cf = new Text();
            Text cq = new Text();
            Mutation mutation = null;
            for (Entry<Key, Value> entry : scanner) {
                entry.getKey().getRow(row);
                entry.getKey().getColumnFamily(cf);
                entry.getKey().getColumnQualifier(cq);

                // if the rows do not match, index the mutation
                if (prevRow != null && !prevRow.equals(row)) {
                    if (!dryRun) {
                        indexer.index(mutation, incrementRowMetric);
                    }

                    ++numRows;
                    mutation = null;

                    if (numRows % 500000 == 0) {
                        if (dryRun) {
                            LOG.info(format("In progress, would have re-indexed %s rows", numRows));
                        }
                        else {
                            LOG.info(format("In progress, re-indexed %s rows", numRows));
                            indexWriter.flush();
                            metricsWriter.flush();
                        }
                    }
                }

                if (mutation == null) {
                    mutation = new Mutation(row);
                }

                mutation.put(cf, cq, entry.getKey().getColumnVisibilityParsed(), entry.getKey().getTimestamp(), entry.getValue());

                if (prevRow == null) {
                    prevRow = new Text(row);
                }
                else {
                    prevRow.set(row);
                }
            }

            // Index the final mutation
            if (mutation != null) {
                if (!dryRun) {
                    indexer.index(mutation, incrementRowMetric);
                }
                ++numRows;
            }

            if (dryRun) {
                LOG.info(format("Finished dry run of rewriting index entries. Would have re-indexed %s rows", numRows));
            }
            else {
                LOG.info(format("Finished adding index entries. Re-indexed %s rows", numRows));
            }
        }
        catch (AccumuloException e) {
            LOG.error("Accumulo exception", e);
        }
        catch (TableNotFoundException e) {
            LOG.error("Table not found, must have been deleted during process", e);
        }
        finally {
            if (indexWriter != null) {
                try {
                    indexWriter.close();
                    metricsWriter.close();
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

    private void deleteIndexEntries(Connector connector, AccumuloTable table, long start)
    {
        LOG.info(format("Scanning index table %s to delete index entries", table.getIndexTableName()));
        BatchScanner scanner = null;
        BatchWriter indexWriter = null;
        try {
            // Create index writer and metrics writer, but we are never going to flush the metrics writer
            indexWriter = connector.createBatchWriter(table.getIndexTableName(), bwc);
            scanner = connector.createBatchScanner(table.getIndexTableName(), auths, 10);
            LOG.info(format("Created batch scanner against %s with auths %s", table.getIndexTableName(), auths));

            IteratorSetting timestampFilter = new IteratorSetting(21, "timestamp", TimestampFilter.class);
            TimestampFilter.setRange(timestampFilter, 0L, start);
            scanner.addScanIterator(timestampFilter);

            setScannerColumns(table, scanner);

            scanner.setRanges(connector.tableOperations().splitRangeByTablets(table.getIndexTableName(), new Range(), MAX_VALUE));

            // Scan the index table, gathering row IDs into batches
            long numTotalMutations = 0L;

            Map<ByteBuffer, RowStatus> rowIdStatuses = new HashMap<>();
            Multimap<ByteBuffer, Mutation> queryIndexEntries = MultimapBuilder.hashKeys().hashSetValues().build();
            Text text = new Text();
            for (Entry<Key, Value> entry : scanner) {
                ++numTotalMutations;

                ByteBuffer rowID = ByteBuffer.wrap(entry.getKey().getColumnQualifier(text).copyBytes());
                Mutation mutation = new Mutation(entry.getKey().getRow(text).copyBytes());
                mutation.putDelete(
                        entry.getKey().getColumnFamily(text).copyBytes(),
                        entry.getKey().getColumnQualifier(text).copyBytes(),
                        entry.getKey().getColumnVisibilityParsed(),
                        start);

                // Get status of this row ID
                switch (rowIdStatuses.getOrDefault(rowID, RowStatus.UNKNOWN)) {
                    case ABSENT:
                    case UNKNOWN:
                        // Absent or unknown? Add it to the collection to check the status and/or delete
                        queryIndexEntries.put(rowID, mutation);
                        break;
                    case PRESENT: // Present? No op
                        break;
                }

                if (queryIndexEntries.size() == 100000) {
                    flushDeleteEntries(connector, table, start, indexWriter, ImmutableMultimap.copyOf(queryIndexEntries), rowIdStatuses);
                    queryIndexEntries.clear();
                }
            }

            flushDeleteEntries(connector, table, start, indexWriter, ImmutableMultimap.copyOf(queryIndexEntries), rowIdStatuses);
            queryIndexEntries.clear();

            LOG.info(format(
                    "Finished scanning index entries. There are %s distinct row IDs containing %s entries. %s rows present in the data table and %s absent",
                    rowIdStatuses.size(),
                    numTotalMutations,
                    rowIdStatuses.entrySet().stream().filter(entry -> entry.getValue().equals(RowStatus.PRESENT)).count(),
                    rowIdStatuses.entrySet().stream().filter(entry -> entry.getValue().equals(RowStatus.ABSENT)).count()));

            if (dryRun) {
                LOG.info(format("Would have deleted %s index entries", numDeletedIndexEntries));
            }
            else {
                LOG.info(format("Deleted %s index entries", numDeletedIndexEntries));
            }
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            LOG.error("Accumulo exception", e);
        }
        catch (TableNotFoundException e) {
            LOG.error("Table not found, must have been deleted during process", e);
        }
        finally {
            if (indexWriter != null) {
                try {
                    indexWriter.close();
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

    private void flushDeleteEntries(Connector connector, AccumuloTable table, long start, BatchWriter indexWriter, Multimap<ByteBuffer, Mutation> queryIndexEntries, Map<ByteBuffer, RowStatus> rowIdStatuses)
            throws MutationsRejectedException, TableNotFoundException
    {
        if (queryIndexEntries.size() > 0) {
            setRowIdStatuses(connector, table, start, queryIndexEntries, rowIdStatuses);

            AtomicLong numDeleteRows = new AtomicLong(0);
            ImmutableList.Builder<Mutation> builder = ImmutableList.builder();
            queryIndexEntries.asMap().entrySet().forEach(entry -> {
                if (rowIdStatuses.get(entry.getKey()) == RowStatus.ABSENT) {
                    builder.addAll(entry.getValue());
                    numDeleteRows.incrementAndGet();
                }
            });
            List<Mutation> deleteMutations = builder.build();

            numDeletedIndexEntries += deleteMutations.size();

            if (!dryRun) {
                indexWriter.addMutations(deleteMutations);
            }
        }
    }

    private void setRowIdStatuses(Connector connector, AccumuloTable table, long timestamp, Multimap<ByteBuffer, Mutation> queryIndexEntries, Map<ByteBuffer, RowStatus> rowIdStatuses)
            throws TableNotFoundException
    {
        // Set ranges to all row IDs that we have no status for
        List<Range> queryRanges = queryIndexEntries.keySet().stream()
                .filter(x -> !rowIdStatuses.containsKey(x))
                .map(x -> new Range(new Text(x.array())))
                .collect(Collectors.toList());

        if (queryRanges.size() == 0) {
            return;
        }

        BatchScanner scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);
        scanner.setRanges(queryRanges);

        IteratorSetting iteratorSetting = new IteratorSetting(MAX_VALUE, TimestampFilter.class);
        TimestampFilter.setEnd(iteratorSetting, timestamp, true);
        scanner.addScanIterator(iteratorSetting);

        scanner.addScanIterator(new IteratorSetting(1, FirstEntryInRowIterator.class));

        // Make a copy of all the row IDs we are querying on to back-fill collection
        Set<ByteBuffer> allRowIDs = new HashSet<>(queryIndexEntries.keySet());

        // Scan the data table, removing all known row IDs and setting their status to present
        Text text = new Text();
        for (Entry<Key, Value> entry : scanner) {
            ByteBuffer rowID = ByteBuffer.wrap(entry.getKey().getRow(text).copyBytes());
            allRowIDs.remove(rowID);

            // Assert that this entry is new
            if (rowIdStatuses.put(rowID, RowStatus.PRESENT) != null) {
                throw new RuntimeException(format("Internal error, row %s already has status", new String(rowID.array(), UTF_8)));
            }
        }
        scanner.close();

        AtomicLong newlyAbsent = new AtomicLong(0);
        // Back-fill the absent map -- rows may already be flagged as absent
        allRowIDs.forEach(rowID -> {
            RowStatus existingStatus = rowIdStatuses.get(rowID);
            if (existingStatus == null) {
                newlyAbsent.incrementAndGet();
                rowIdStatuses.put(rowID, RowStatus.ABSENT);
            }
            else if (existingStatus == RowStatus.PRESENT) {
                throw new RuntimeException(format("Internal error, row %s already has PRESENT status", new String(rowID.array(), UTF_8)));
            }
        });
    }

    private void setScannerColumns(AccumuloTable table, Scanner scanner)
    {
        if (columns.isPresent()) {
            for (String column : columns.get()) {
                Optional<AccumuloColumnHandle> handle = table.getColumns().stream().filter(x -> x.getName().equals(column)).findAny();
                if (!handle.isPresent()) {
                    throw new InvalidParameterException(format("No column named %s found in table %s", column, tableName));
                }

                if (handle.get().getFamily().isPresent() && handle.get().getQualifier().isPresent()) {
                    LOG.info(format("Fetching column %s:%s for table %s", handle.get().getFamily(), handle.get().getQualifier(), tableName));
                    scanner.fetchColumn(new Text(handle.get().getFamily().get()), new Text(handle.get().getQualifier().get()));
                }
                else {
                    throw new InvalidParameterException(format("Column %s in table %s is the row ID column and is therefore not indexed", column, tableName));
                }
            }
        }
    }

    private void setScannerColumns(AccumuloTable table, BatchScanner scanner)
    {
        if (columns.isPresent()) {
            for (String column : columns.get()) {
                Optional<AccumuloColumnHandle> handle = table.getColumns().stream().filter(x -> x.getName().equals(column)).findAny();
                if (!handle.isPresent()) {
                    throw new InvalidParameterException(format("No column named %s found in table %s", column, tableName));
                }

                if (handle.get().getFamily().isPresent() && handle.get().getQualifier().isPresent()) {
                    LOG.info(format("Fetching column %s:%s for table %s", handle.get().getFamily().get(), handle.get().getQualifier().get(), tableName));
                    scanner.fetchColumn(new Text(handle.get().getFamily().get()), new Text(handle.get().getQualifier().get()));
                }
                else {
                    throw new InvalidParameterException(format("Column %s in table %s is the row ID column and is not indexed", column, tableName));
                }
            }
        }
    }

    private enum RowStatus
    {
        PRESENT,
        ABSENT,
        UNKNOWN
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
        this.setDryRun(!cmd.hasOption(FORCE_OPT));
        this.setAddOnly(cmd.hasOption(ADD_ONLY_OPT));

        if (cmd.hasOption(COLUMNS_OPT)) {
            this.setColumns(Optional.of(ImmutableList.copyOf(cmd.getOptionValues(COLUMNS_OPT))));
        }

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

    public void setDryRun(boolean dryRun)
    {
        this.dryRun = dryRun;
    }

    public void setAddOnly(boolean addOnly)
    {
        this.addOnly = addOnly;
    }

    public void setColumns(Optional<List<String>> columns)
    {
        this.columns = columns;
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
        opts.addOption(
                OptionBuilder
                        .withLongOpt(FORCE_OPT)
                        .withDescription("Force deleting of entries. Default is a dry run")
                        .create());
        opts.addOption(
                OptionBuilder
                        .withLongOpt(ADD_ONLY_OPT)
                        .withDescription("Only add index entries, do not delete them or run the rewrite metrics tool.  Requires --force to do anything.  Default is to add and delete.")
                        .create());
        opts.addOption(
                OptionBuilder
                        .withLongOpt("columns")
                        .withDescription("Specific list of Presto columns ot index.  Default is to index all columns")
                        .hasArgs()
                        .create(COLUMNS_OPT));
        return opts;
    }
}
