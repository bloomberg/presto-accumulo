package bloomberg.presto.accumulo.benchmark;

import java.io.File;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.AccumuloConfig;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 11) {
            System.err.println(
                    "Usage: [instance] [zookeepers] [user] [passwd] [dbgen.dir] [presto.host] [presto.port] [benchmark.dir] [csv.schemas] [num.splits] [timeout]");
            return 1;
        }

        AccumuloConfig accConf = new AccumuloConfig();
        accConf.setInstance(args[0]).setZooKeepers(args[1]).setUsername(args[2])
                .setPassword(args[3]);
        File dbgendir = new File(args[4]);
        String host = args[5];
        int port = Integer.parseInt(args[6]);
        File benchmarkDir = new File(args[7]);
        File scriptsDir = new File(benchmarkDir, "scripts");

        List<Pair<String, Float>> schemaScalePairs = Arrays
                .asList(args[8].split(",")).stream().map(x -> x.split(":"))
                .map(x -> Pair.of(x[0], Float.parseFloat(x[1])))
                .collect(Collectors.toList());

        List<Integer> numSplits = Arrays.asList(args[9].split(",")).stream()
                .map(x -> Integer.parseInt(x)).collect(Collectors.toList());

        int timeout = Integer.parseInt(args[10]);

        if (!dbgendir.exists() || dbgendir.isFile()) {
            throw new InvalidParameterException(
                    dbgendir + " does not exist or is not a directory");
        }

        if (!scriptsDir.exists() || scriptsDir.isFile()) {
            throw new InvalidParameterException(
                    scriptsDir + " does not exist or is not a directory");
        }

        String[] splittableTables = { "customer", "lineitem", "orders", "part",
                "partsupp", "supplier" };

        final List<QueryMetrics> metrics = new ArrayList<>();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                System.out.println(QueryMetrics.getHeader());
                for (QueryMetrics qm : metrics) {
                    System.out.println(qm);
                }
            }
        }));

        int numRounds = 16 * numSplits.size() * schemaScalePairs.size();
        int ranRounds = 0;
        // for each schema
        for (Pair<String, Float> s : schemaScalePairs) {
            String schema = s.getLeft();
            float scale = s.getRight();
            TpchDBGenInvoker.run(dbgendir, scale);
            TpchDBGenIngest.run(accConf, schema, dbgendir);
            QueryFormatter.run(s.getLeft(), benchmarkDir);

            // for each number of splits
            for (int ns : numSplits) {
                // split each table
                for (String tableName : splittableTables) {
                    Splitter.run(accConf, schema, tableName, scale, ns);
                }

                // Run queries flipping all of the optimization flags on and off
                boolean[] bvalues = { false, true };
                for (boolean optimizeColumnFiltersEnabled : bvalues) {
                    for (boolean optimizeRangePredicatePushdownEnabled : bvalues) {
                        for (boolean optimizeRangeSplitsEnabled : bvalues) {
                            for (boolean secondaryIndexEnabled : bvalues) {
                                runQuery(accConf, host, port, schema,
                                        scriptsDir, scale, ns, metrics,
                                        optimizeColumnFiltersEnabled,
                                        optimizeRangePredicatePushdownEnabled,
                                        optimizeRangeSplitsEnabled,
                                        secondaryIndexEnabled, timeout);
                                ++ranRounds;
                                System.out.println(String
                                        .format("Ran rounds: %d\tTotal rounds: %d\tProgress: %2f",
                                                ranRounds, numRounds,
                                                +((float) ranRounds
                                                        / (float) numRounds
                                                        * 100.0f)));
                            }
                        }
                    }
                }

                System.out.println(QueryMetrics.getHeader());
                for (QueryMetrics t : metrics) {
                    System.out.println(t);
                }

                // Merge tables
                for (String tableName : splittableTables) {
                    Merger.run(accConf,
                            AccumuloClient.getFullTableName(schema, tableName));
                }
            }
        }

        return 0;
    }

    private void runQuery(AccumuloConfig accConf, String host, int port,
            String schema, File scriptsDir, float scale, int numSplits,
            List<QueryMetrics> metrics, boolean optimizeColumnFiltersEnabled,
            boolean optimizeRangePredicatePushdownEnabled,
            boolean optimizeRangeSplitsEnabled, boolean secondaryIndexEnabled,
            int timeout) throws Exception {

        List<QueryMetrics> qm = TpchQueryExecutor.run(accConf, host, port,
                schema, scriptsDir, optimizeColumnFiltersEnabled,
                optimizeRangePredicatePushdownEnabled,
                optimizeRangeSplitsEnabled, secondaryIndexEnabled, timeout);

        qm.stream().forEach(x ->
            {
                x.scale = scale;
                x.numAccumuloSplits = numSplits;
                x.schema = schema;
            });

        metrics.addAll(qm);

    }
}
