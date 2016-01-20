package bloomberg.presto.accumulo.benchmark;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bloomberg.presto.accumulo.AccumuloConfig;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 6) {
            System.err.println(
                    "Usage: [instance] [zookeepers] [user] [passwd] [presto.schema] [dbgen.dir]");
            return 1;
        }

        AccumuloConfig accConf = new AccumuloConfig();
        accConf.setInstance(args[0]).setZooKeepers(args[1]).setUsername(args[2])
                .setPassword(args[3]);

        String schema = args[4];
        float scale = schema.equals("tiny") ? .01f
                : (schema.equals("small") ? .1f
                        : Float.parseFloat(schema.replaceAll("\\D", "")));
        File dbgendir = new File(args[5]);

        TpchDBGenInvoker.run(dbgendir, scale);
        TpchDBGenIngest.run(accConf, schema, dbgendir);

        return 0;
    }

}
