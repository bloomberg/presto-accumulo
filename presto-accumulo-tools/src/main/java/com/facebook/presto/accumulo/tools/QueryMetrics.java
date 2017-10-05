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

import com.facebook.presto.accumulo.AccumuloEventListener;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.MethodHandleUtil;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.tracer.SpanTree;
import org.apache.accumulo.tracer.TraceFormatter;
import org.apache.accumulo.tracer.thrift.Annotation;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This task scans the Presto query metrics table (which is populated by the AccumuloEventListener)
 * as well as the Trace table to
 */
public class QueryMetrics
        extends Task
{
    public static final String TASK_NAME = "query-metrics";
    public static final String DESCRIPTION = "Queries the metrics and trace tables for information regarding a Presto query";

    private static final AccumuloRowSerializer SERIALIZER = new LexicoderRowSerializer();
    private static final String QUERY_ARCHIVE_TABLE = "presto.query_archive";

    private static final Logger LOG = Logger.getLogger(QueryMetrics.class);

    // Options
    private static final String QUERY_ID_OPT = "q";

    // User-configured values
    private AccumuloConfig config;
    private String queryId;

    private static final Map<String, Type> columnToType;

    static {
        ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
        builder.put("query:id", VARCHAR)
                .put("query:source", VARCHAR)
                .put("query:user", VARCHAR)
                .put("query:user_agent", VARCHAR)
                .put("query:sql", VARCHAR)
                .put("query:session_properties", new MapType(
                        VARCHAR,
                        VARCHAR,
                        MethodHandleUtil.methodHandle(Slice.class, "equals", Object.class).asType(MethodType.methodType(boolean.class, Slice.class, Slice.class)),
                        MethodHandleUtil.methodHandle(Slice.class, "hashCode").asType(MethodType.methodType(long.class, Slice.class)),
                        MethodHandleUtil.methodHandle(AccumuloEventListener.class, "blockVarcharHashCode", Block.class, int.class)))
                .put("query:schema", VARCHAR)
                .put("query:transaction_id", VARCHAR)
                .put("query:state", VARCHAR)
                .put("query:is_complete", BOOLEAN)
                .put("query:uri", VARCHAR)
                .put("query:create_time", TIMESTAMP)
                .put("query:start_time", TIMESTAMP)
                .put("query:end_time", TIMESTAMP)
                .put("query:cpu_time", BIGINT)
                .put("query:analysis_time", BIGINT)
                .put("query:planning_time", BIGINT)
                .put("query:queued_time", BIGINT)
                .put("query:wall_time", BIGINT)
                .put("query:completed_splits", INTEGER)
                .put("query:total_bytes", BIGINT)
                .put("query:total_rows", BIGINT)
                .put("query:peak_mem_bytes", BIGINT)
                .put("query:environment", VARCHAR)
                .put("query:remote_client_address", VARCHAR)
                .put("query:server_address", VARCHAR)
                .put("query:server_version", VARCHAR)
                .put("input:input", new ArrayType(new RowType(
                        ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR),
                        Optional.of(ImmutableList.of("columns", "connector_id", "schema", "table", "connector_info")))))
                .put("output:connector_id", VARCHAR)
                .put("output:schema", VARCHAR)
                .put("output:table", VARCHAR)
                .put("failure:failure", new RowType(
                        ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR),
                        Optional.of(ImmutableList.of("code", "type", "msg", "host", "json"))))
                .put("split:split", new ArrayType(
                        new RowType(
                                ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, VARCHAR, VARCHAR, BIGINT, VARCHAR, TIMESTAMP, VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT),
                                Optional.of(ImmutableList.of("completed_data_size_bytes", "completed_positions", "completed_read_time", "cpu_time", "create_time", "end_time", "failure_msg", "failure_type", "queued_time", "stage_id", "start_time", "task_id", "time_to_first_byte", "time_to_last_byte", "user_time", "wall_time")))))
                .build();
        columnToType = builder.build();
    }

    public int exec()
            throws Exception
    {
        // Validate the required parameters have been set
        int numErrors = checkParam(config, "config");
        numErrors += checkParam(queryId, "queryId");
        if (numErrors > 0) {
            return 1;
        }

        // Create the instance and the connector
        Instance inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        Connector connector = inst.getConnector(config.getUsername(), new PasswordToken(config.getPassword()));

        Scanner scanner = null;
        try {
            scanner = connector.createScanner(QUERY_ARCHIVE_TABLE, new Authorizations());
            scanner.setRange(new Range(queryId));

            boolean found = false;
            Text text = new Text();
            for (Entry<Key, Value> entry : scanner) {
                found = true;
                String column = entry.getKey().getColumnFamily(text).toString() + ":" + entry.getKey().getColumnQualifier(text).toString();
                Type type = columnToType.get(column);
                if (type == null) {
                    throw new RuntimeException("No type for column " + column);
                }
                System.out.println(column + " " + SERIALIZER.decode(type, entry.getValue().get()));
            }

            if (!found) {
                System.out.println("No info found in query archive for " + queryId);
            }
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        Map<RemoteSpan, Range> traceIds = getTraceIds(connector, queryId);

        List<Callable<Triple<RemoteSpan, Long, SpanTree>>> tasks = new ArrayList<>();
        traceIds.entrySet().forEach(span ->
                tasks.add(() -> {
                    Scanner traceScanner = null;
                    try {
                        traceScanner = connector.createScanner("trace", new Authorizations());
                        traceScanner.setRange(span.getValue());
                        AtomicLong start = new AtomicLong(Long.MAX_VALUE);
                        SpanTree tree = new SpanTree();
                        traceScanner.forEach(entry -> {
                            RemoteSpan traceSpan = TraceFormatter.getRemoteSpan(entry);
                            tree.addNode(traceSpan);
                            start.set(Math.min(start.get(), traceSpan.getStart()));
                        });

                        return Triple.of(span.getKey(), start.get(), tree);
                    }
                    finally {
                        if (traceScanner != null) {
                            traceScanner.close();
                        }
                    }
                }));

        ExecutorService service = getExitingExecutorService(new ThreadPoolExecutor(tasks.size(), tasks.size(), 0L, SECONDS, new SynchronousQueue<>()));

        service.invokeAll(tasks).stream().map(task -> {
            try {
                return task.get();
            }
            catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException("Task execution failed or was interrupted", e);
            }
        }).sorted(Comparator.comparingLong(o -> o.getLeft().getStart()))
                .map(result -> {
                    StringBuilder sb = new StringBuilder();
                    result.getRight().visit((level, parent, node, children) -> appendRow(sb, level, node, result.getMiddle()));
                    return sb.toString();
                })
                .limit(2L)
                .forEachOrdered(System.out::println);

        return 0;
    }

    private static void appendRow(StringBuilder sb, int level, RemoteSpan node, long finalStart)
    {
        sb.append(format("%d+\t%d", node.stop - node.start, node.start - finalStart));
        sb.append(format("%s%s@%s", StringUtils.repeat('\t', level), node.svc, node.sender));
        sb.append(node.description);
        boolean hasData = node.data != null && !node.data.isEmpty();
        boolean hasAnnotations = node.annotations != null && !node.annotations.isEmpty();
        sb.append("\n");
        if (hasData || hasAnnotations) {
            if (hasData) {
                for (Entry<String, String> entry : node.data.entrySet()) {
                    sb.append(format("%s\t%s", entry.getKey(), entry.getValue()));
                }
            }
            if (hasAnnotations) {
                for (Annotation entry : node.annotations) {
                    sb.append(format("%s\t%s", entry.getMsg(), entry.getTime() - finalStart));
                }
            }
        }
    }

    private Map<RemoteSpan, Range> getTraceIds(Connector connector, String queryId)
            throws TableNotFoundException
    {
        Scanner scanner = null;
        try {
            scanner = connector.createScanner("trace", new Authorizations());
            scanner.setRange(new Range("start:" + Long.toHexString(0), "start:" + Long.toHexString(Long.MAX_VALUE)));

            return StreamSupport.stream(scanner.spliterator(), false) // Turn into stream
                    .map(TraceFormatter::getRemoteSpan) // Map to RemoteSpan
                    .filter(span -> span.getDescription().contains(queryId)) // Filter by description
                    .map(span -> Pair.of(span, new Range(Long.toHexString(span.getTraceId())))) // Convert trace ID to Range
                    .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    @Override
    public int run(AccumuloConfig config, CommandLine cmd)
            throws Exception
    {
        this.setConfig(config);
        this.setQueryId(cmd.getOptionValue(QUERY_ID_OPT));
        return this.exec();
    }

    public void setConfig(AccumuloConfig config)
    {
        this.config = config;
    }

    public void setQueryId(String queryId)
    {
        this.queryId = queryId;
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
                        .withLongOpt("query-id")
                        .withDescription("Presto query ID")
                        .hasArg()
                        .create(QUERY_ID_OPT));
        return opts;
    }
}
