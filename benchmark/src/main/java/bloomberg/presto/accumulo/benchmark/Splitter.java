package bloomberg.presto.accumulo.benchmark;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;

import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloTable;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;

public class Splitter {

    public int run(AccumuloConfig conf, String tableName, String[] splits)
            throws Exception {

        ZooKeeperInstance inst = new ZooKeeperInstance(conf.getInstance(),
                conf.getZooKeepers());
        Connector conn = inst.getConnector(conf.getUsername(),
                new PasswordToken(conf.getPassword()));

        AccumuloTable table = AccumuloMetadataManager
                .getDefault("accumulo", conf)
                .getTable(SchemaTableName.valueOf(tableName));

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
                tableSplits.add(new Text(LexicoderRowSerializer
                        .encode(rowIdType, Long.parseLong(s))));
            } else {
                throw new UnsupportedOperationException(
                        "Type " + rowIdType + " is not supported");
            }
        }

        conn.tableOperations().addSplits(tableName, tableSplits);

        System.out.println("Splits added, compacting");
        conn.tableOperations().compact(tableName, null, null, true, true);

        System.out.println("Splits are ");
        for (Text s : conn.tableOperations().listSplits(tableName)) {
            if (rowIdType == BigintType.BIGINT) {
                System.out.println((Long) LexicoderRowSerializer
                        .decode(rowIdType, s.copyBytes()));
            } else {
                throw new UnsupportedOperationException(
                        "Type " + rowIdType + " is not supported");
            }
        }

        return 0;
    }
}
