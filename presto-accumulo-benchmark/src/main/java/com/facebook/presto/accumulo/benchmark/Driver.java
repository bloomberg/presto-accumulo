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
package com.facebook.presto.accumulo.benchmark;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Driver
        extends Configured
        implements Tool
{

    private static final Logger LOG = Logger.getLogger(Driver.class);
    private static List<String> BLACKLIST = ImmutableList.copyOf(new String[] {"2.sql", "4.sql", "9.sql", "11.sql", "13.sql", "15.sql", "17.sql", "19.sql", "20.sql", "21.sql", "22.sql"});
    private int numQueries = 0;
    private int ranQueries = 0;
    private List<QueryMetrics> metrics = new ArrayList<>();
    private List<File> queryFiles;

    public static void main(String[] args)
            throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    @Override
    public int run(String[] args)
            throws Exception
    {

        if (args.length != 12) {
            System.err.println("Usage: [instance] [zookeepers] [user] [passwd] [dbgen.dir] [presto.host] [presto.port] [benchmark.dir] [csv.schemas] [num.splits] [timeout] [skip.ingest]");
            return 1;
        }

        AccumuloConfig accConf = new AccumuloConfig();
        accConf.setInstance(args[0]).setZooKeepers(args[1]).setUsername(args[2]).setPassword(args[3]);
        File dbgendir = new File(args[4]);
        String host = args[5];
        int port = Integer.parseInt(args[6]);
        File benchmarkDir = new File(args[7]);
        File scriptsDir = new File(benchmarkDir, "scripts");

        List<Pair<String, Float>> schemaScalePairs = Arrays.asList(args[8].split(",")).stream().map(x -> x.split(":")).map(x -> Pair.of(x[0], Float.parseFloat(x[1]))).collect(Collectors.toList());

        List<Integer> numSplits = Arrays.asList(args[9].split(",")).stream().map(x -> Integer.parseInt(x)).collect(Collectors.toList());

        int timeout = Integer.parseInt(args[10]);
        boolean skipIngest = Boolean.parseBoolean(args[11]);

        if (!dbgendir.exists() || dbgendir.isFile()) {
            throw new InvalidParameterException(dbgendir + " does not exist or is not a directory");
        }

        if (!scriptsDir.exists() || scriptsDir.isFile()) {
            throw new InvalidParameterException(scriptsDir + " does not exist or is not a directory");
        }

        queryFiles = Arrays.asList(scriptsDir.listFiles()).stream().filter(x -> !BLACKLIST.contains(x.getName()) && x.getName().matches("[0-9]+.sql")).collect(Collectors.toList());

        String[] splittableTables = {"customer", "lineitem", "orders", "part", "partsupp", "supplier"};

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
        {

            @Override
            public void run()
            {
                System.out.println(QueryMetrics.getHeader());
                for (QueryMetrics qm : metrics) {
                    System.out.println(qm);
                }
            }
        }));

        numQueries = 4 * numSplits.size() * schemaScalePairs.size() * queryFiles.size();
        ranQueries = 0;
        
        // for each schema
        for (Pair<String, Float> s : schemaScalePairs) {
            String schema = s.getLeft();
            float scale = s.getRight();

            if (!skipIngest) {
                TpchDBGenInvoker.run(dbgendir, scale);
                TpchDBGenIngest.run(accConf, schema, dbgendir);
            }

            QueryFormatter.run(s.getLeft(), benchmarkDir);

            // for each number of splits
            for (int ns : numSplits) {
                // split each table
                for (String tableName : splittableTables) {
                    Splitter.run(accConf, schema, tableName, ns);
                }

                // Run queries flipping all of the optimization flags on and off
                boolean[] bvalues = {false, true};
                for (boolean optimizeRangeSplitsEnabled : bvalues) {
                    for (boolean secondaryIndexEnabled : bvalues) {
                        runQueries(accConf, host, port, schema, scale, ns, optimizeRangeSplitsEnabled, secondaryIndexEnabled, timeout);
                    }
                }

                // Merge tables
                for (String tableName : splittableTables) {
                    Merger.run(accConf, AccumuloTable.getFullTableName(schema, tableName));
                }
            }
        }

        return 0;
    }

    private void runQueries(AccumuloConfig accConf, String host, int port, String schema, float scale, int numSplits, boolean optimizeRangeSplitsEnabled, boolean secondaryIndexEnabled, int timeout)
            throws Exception
    {

        for (File qf : queryFiles) {
            QueryMetrics qm = TpchQueryExecutor.run(accConf, qf, host, port, schema, optimizeRangeSplitsEnabled, secondaryIndexEnabled, timeout);

            qm.scale = scale;
            qm.numAccumuloSplits = numSplits;
            qm.schema = schema;
            metrics.add(qm);

            LOG.info(qm);
            LOG.info(format("Query Progress: Executed %d\tTotal: %d\tProgress: %2f", ++ranQueries, numQueries, +((float) ranQueries / (float) numQueries * 100.0f)));
        }
    }
}
