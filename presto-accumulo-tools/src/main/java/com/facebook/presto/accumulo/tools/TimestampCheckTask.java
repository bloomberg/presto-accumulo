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
import com.facebook.presto.accumulo.metadata.AccumuloMetadataManager;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This task scans the index table of an Accumulo table, re-writing the metrics
 */
public class TimestampCheckTask
        extends Task
{
    public static final String TASK_NAME = "timestamp-check";
    public static final String DESCRIPTION = "Scans the metrics, index, and data tables for the number of entries in a timespan";

    private static final DateTimeFormatter PARSER = ISODateTimeFormat.dateTimeParser();
    private static final Logger LOG = Logger.getLogger(TimestampCheckTask.class);

    // Options
    private static final char SCHEMA_OPT = 's';
    private static final char TABLE_OPT = 't';
    private static final char AUTHORIZATIONS_OPT = 'a';
    private static final String START_OPT = "st";
    private static final char END_OPT = 'e';
    private static final String COLUMN_OPT = "col";

    // User-configured values
    private AccumuloConfig config;
    private Authorizations auths;
    private String schema;
    private String tableName;
    private String start;
    private String end;
    private byte[] startBytes;
    private byte[] endBytes;
    private Range range;
    private String column;

    public int exec()
            throws Exception
    {
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

        AccumuloRowSerializer serializer = new LexicoderRowSerializer();

        startBytes = serializer.encode(TimestampType.TIMESTAMP, PARSER.parseDateTime(start).getMillis());
        endBytes = serializer.encode(TimestampType.TIMESTAMP, PARSER.parseDateTime(end).getMillis());

        this.range = new Range(
                new Text(startBytes),
                new Text(endBytes)
        );

        long timestamp = System.currentTimeMillis();

        Optional<AccumuloColumnHandle> columnHandle = table.getColumns().stream().filter(handle -> handle.getName().equalsIgnoreCase(column)).findAny();
        checkArgument(columnHandle.isPresent(), "no column found");

        ExecutorService service = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(3, 3, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>()));

        List<Future<Void>> tasks = service.invokeAll(
                ImmutableList.of(() -> {
                    getDataCount(connector, table, columnHandle.get(), timestamp);
                    return null;
                }, () -> {
                    getIndexCount(connector, table, columnHandle.get(), timestamp);
                    return null;
                }, () -> {
                    getMetricCount(connector, table, columnHandle.get(), timestamp);
                    return null;
                }));

        for (Future<Void> task : tasks) {
            task.get();
        }

        LOG.info("Finished");
        return 0;
    }

    private void getDataCount(Connector connector, AccumuloTable table, AccumuloColumnHandle column, long timestamp)
            throws Exception
    {
        LOG.info("Getting data count");

        BatchScanner scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);
        scanner.setRanges(connector.tableOperations().splitRangeByTablets(table.getFullTableName(), new Range(), Integer.MAX_VALUE));
        scanner.fetchColumn(new Text(column.getFamily().get()), new Text(column.getQualifier().get()));

        IteratorSetting iteratorSetting = new IteratorSetting(Integer.MAX_VALUE, TimestampFilter.class);
        TimestampFilter.setEnd(iteratorSetting, timestamp, true);
        scanner.addScanIterator(iteratorSetting);

        Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

        long numRows = 0;
        for (Entry<Key, Value> entry : scanner) {
            byte[] timestampValue = entry.getValue().get();
            if (comparator.compare(timestampValue, startBytes) >= 0 && comparator.compare(timestampValue, endBytes) <= 0) {
                ++numRows;
            }
        }
        scanner.close();

        LOG.info("Number of rows from data table is " + numRows);
    }

    private void getIndexCount(Connector connector, AccumuloTable table, AccumuloColumnHandle column, long timestamp)
            throws Exception
    {
        LOG.info("Getting index count");
        BatchScanner scanner = connector.createBatchScanner(table.getIndexTableName(), auths, 10);
        scanner.setRanges(connector.tableOperations().splitRangeByTablets(table.getIndexTableName(), range, Integer.MAX_VALUE));
        scanner.fetchColumnFamily(new Text(Indexer.getIndexColumnFamily(column.getFamily().get().getBytes(UTF_8), column.getQualifier().get().getBytes(UTF_8)).array()));

        IteratorSetting iteratorSetting = new IteratorSetting(Integer.MAX_VALUE, TimestampFilter.class);
        TimestampFilter.setEnd(iteratorSetting, timestamp, true);
        scanner.addScanIterator(iteratorSetting);

        Text text = new Text();
        ImmutableSet.Builder<Range> indexRanges = ImmutableSet.builder();
        ImmutableSet.Builder<ByteBuffer> indexRowIDs = ImmutableSet.builder();
        long numRows = 0;
        for (Entry<Key, Value> entry : scanner) {
            indexRanges.add(new Range(entry.getKey().getColumnQualifier()));
            indexRowIDs.add(ByteBuffer.wrap(entry.getKey().getColumnQualifier(text).copyBytes()));
            ++numRows;
        }
        scanner.close();

        LOG.info("Number of rows in index table is " + numRows);

        getCountViaIndex(connector, table, column, timestamp, indexRanges.build(), indexRowIDs.build());
    }

    private void getCountViaIndex(Connector connector, AccumuloTable table, AccumuloColumnHandle column, long timestamp, Set<Range> indexRanges, Set<ByteBuffer> indexRowIDs)
            throws Exception
    {
        LOG.info("Number of index ranges is " + indexRanges.size());
        LOG.info("Number of distinct index ranges is " + indexRanges.stream().distinct().count());

        // Scan table with these entries
        Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
        BatchScanner scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);
        scanner.setRanges(indexRanges);
        scanner.fetchColumn(new Text(column.getFamily().get()), new Text(column.getQualifier().get()));

        IteratorSetting iteratorSetting = new IteratorSetting(Integer.MAX_VALUE, TimestampFilter.class);
        TimestampFilter.setEnd(iteratorSetting, timestamp, true);
        scanner.addScanIterator(iteratorSetting);

        Set<ByteBuffer> values = new HashSet<>(indexRowIDs);

        Text text = new Text();
        long numRows = 0;
        long outsideRange = 0;
        for (Entry<Key, Value> entry : scanner) {
            values.remove(ByteBuffer.wrap(entry.getKey().getRow(text).copyBytes()));

            byte[] timestampValue = entry.getValue().get();
            if (comparator.compare(timestampValue, startBytes) >= 0 && comparator.compare(timestampValue, endBytes) <= 0) {
                ++numRows;
            }
            else {
                ++outsideRange;
            }
        }
        scanner.close();

        LOG.info("Number of rows from data table via index is " + numRows);
        LOG.info("Number of rows from data table outside the time range is " + outsideRange);
        LOG.info("Number of rows in the index not scanned from the table is " + values.size());

        if (values.size() > 0) {
            LOG.info("Sample records:");
            values.stream().limit(10).forEach(x -> LOG.info(new String(x.array(), UTF_8)));
        }
    }

    private void getMetricCount(Connector connector, AccumuloTable table, AccumuloColumnHandle column, long timestamp)
            throws Exception
    {
        LOG.info("Getting metric count");
        BatchScanner scanner = connector.createBatchScanner(table.getIndexTableName() + "_metrics", auths, 10);
        scanner.setRanges(connector.tableOperations().splitRangeByTablets(table.getIndexTableName() + "_metrics", range, Integer.MAX_VALUE));
        scanner.fetchColumnFamily(new Text(Indexer.getIndexColumnFamily(column.getFamily().get().getBytes(UTF_8), column.getQualifier().get().getBytes(UTF_8)).array()));

        IteratorSetting iteratorSetting = new IteratorSetting(Integer.MAX_VALUE, TimestampFilter.class);
        TimestampFilter.setEnd(iteratorSetting, timestamp, true);
        scanner.addScanIterator(iteratorSetting);

        long numRows = 0;
        for (Entry<Key, Value> entry : scanner) {
            numRows += Long.parseLong(new String(entry.getValue().get(), UTF_8));
        }
        scanner.close();

        LOG.info("Number of rows from metrics table is " + numRows);
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
        this.setStart(cmd.getOptionValue(START_OPT));
        this.setEnd(cmd.getOptionValue(END_OPT));
        this.setColumn(cmd.getOptionValue(COLUMN_OPT));

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

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public void setStart(String start)
    {
        this.start = start;
    }

    public void setEnd(String end)
    {
        this.end = end;
    }

    public void setColumn(String column)
    {
        this.column = column;
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
                        .withLongOpt("start")
                        .withDescription("Start timestamp in YYYY-MM-ddTHH:mm:ss.SSS+ZZZZ format")
                        .hasArg()
                        .isRequired()
                        .create(START_OPT));
        opts.addOption(
                OptionBuilder
                        .withLongOpt("end")
                        .withDescription("End timestamp in YYYY-MM-ddTHH:mm:ss.SSS+ZZZZ format")
                        .hasArg()
                        .isRequired()
                        .create(END_OPT));
        opts.addOption(
                OptionBuilder
                        .withLongOpt("column")
                        .withDescription("Presto column name")
                        .hasArg()
                        .isRequired()
                        .create(COLUMN_OPT));
        return opts;
    }
}
