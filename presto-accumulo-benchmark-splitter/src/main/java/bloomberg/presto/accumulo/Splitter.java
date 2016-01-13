package bloomberg.presto.accumulo;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;

public class Splitter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Splitter(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 6) {
            System.err
                    .println("Usage: [instance] [zookeepers] [user] [passwd] [table] [csv.splits]");

            return 1;
        }

        String instance = args[0];
        String zookeepers = args[1];
        String user = args[2];
        String passwd = args[3];
        String tableName = args[4];
        String[] splits = args[5].split(",");

        AccumuloConfig accConfig = new AccumuloConfig();
        accConfig.setInstance(instance);
        accConfig.setPassword(passwd);
        accConfig.setUsername(user);
        accConfig.setZooKeepers(zookeepers);

        ZooKeeperInstance inst = new ZooKeeperInstance(instance, zookeepers);
        Connector conn = inst.getConnector(user, new PasswordToken(passwd));

        AccumuloTable table = AccumuloMetadataManager.getDefault("accumulo",
                accConfig).getTable(SchemaTableName.valueOf(tableName));

        Type rowIdType = null;

        for (AccumuloColumnHandle ach : table.getColumns()) {
            if (ach.getName().equals(table.getRowIdName())) {
                rowIdType = ach.getType();
                break;
            }
        }

        SortedSet<Text> tableSplits = new TreeSet<>();
        for (String s : splits) {
            if (rowIdType == BigintType.BIGINT) {
                tableSplits.add(new Text(LexicoderRowSerializer.encode(
                        rowIdType, Long.parseLong(s))));
            } else {
                throw new UnsupportedOperationException("Type " + rowIdType
                        + " is not supported");
            }
        }

        conn.tableOperations().addSplits(tableName, tableSplits);

        System.out.println("Splits added, compacting");
        conn.tableOperations().compact(tableName, null, null, true, true);

        System.out.println("Splits are ");
        for (Text s : conn.tableOperations().listSplits(tableName)) {
            if (rowIdType == BigintType.BIGINT) {
                System.out.println((Long) LexicoderRowSerializer.decode(
                        rowIdType, s.copyBytes()));
            } else {
                throw new UnsupportedOperationException("Type " + rowIdType
                        + " is not supported");
            }
        }

        return 0;
    }
}
