package bloomberg.presto.accumulo.benchmark;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import bloomberg.presto.accumulo.AccumuloConfig;

public class Merger {

    public static void run(AccumuloConfig conf, String tableName)
            throws Exception {

        ZooKeeperInstance inst = new ZooKeeperInstance(conf.getInstance(),
                conf.getZooKeepers());
        Connector conn = inst.getConnector(conf.getUsername(),
                new PasswordToken(conf.getPassword()));

        conn.tableOperations().merge(tableName, null, null);
    }
}
