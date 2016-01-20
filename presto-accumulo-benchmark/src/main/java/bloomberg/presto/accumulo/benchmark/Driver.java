package bloomberg.presto.accumulo.benchmark;

import java.io.File;
import java.security.InvalidParameterException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableList;

import bloomberg.presto.accumulo.AccumuloConfig;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 8) {
            System.err.println(
                    "Usage: [instance] [zookeepers] [user] [passwd] [dbgen.dir] [host] [port] [scripts.dir]");
            return 1;
        }

        AccumuloConfig accConf = new AccumuloConfig();
        accConf.setInstance(args[0]).setZooKeepers(args[1]).setUsername(args[2])
                .setPassword(args[3]);
        File dbgendir = new File(args[4]);
        String host = args[5];
        int port = Integer.parseInt(args[6]);
        File scriptsDir = new File(args[7]);

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

        ImmutableList<Pair<String, Float>> scales = ImmutableList.of(
                Pair.of("tiny", .01f), Pair.of("small", .1f),
                Pair.of("sf1", 1f), Pair.of("sf10", 10f),
                Pair.of("sf100", 100f));

        Integer[] numSplits = { 0, 1, 3, 5, 9, 15 };
        // for each schema
        for (Pair<String, Float> s : scales) {
            TpchDBGenInvoker.run(dbgendir, s.getRight());
            TpchDBGenIngest.run(accConf, s.getLeft(), dbgendir);

            // for each number of splits
            for (int ns : numSplits) {
                // split each table
                for (String tableName : splittableTables) {
                    Splitter.run(accConf, tableName, s.getRight(), ns);
                }

                // Run queries
                TpchQueryExecutor.run(accConf, host, port, s.getLeft(),
                        scriptsDir);

                // Merge tables
                for (String tableName : splittableTables) {
                    Merger.run(accConf, tableName);
                }
            }
        }

        return 0;
    }

}
