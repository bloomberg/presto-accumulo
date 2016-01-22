package bloomberg.presto.accumulo.benchmark;

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloTable;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Splitter
{

    private static Map<String, Long> MAX_ROW_ID_BY_TABLE = ImmutableMap.<String, Long>builder().put("customer", 150000L).put("lineitem", 6000000L).put("nation", 24L).put("orders", 6000000L).put("part", 200000L).put("partsupp", 200000L).put("region", 4L).put("supplier", 10000L).build();

    private static Map<String, Boolean> TABLE_SCALES = ImmutableMap.<String, Boolean>builder().put("customer", true).put("lineitem", true).put("nation", false).put("orders", true).put("part", true).put("partsupp", true).put("region", false).put("supplier", true).build();

    public static void run(AccumuloConfig conf, String schemaName, String tableName, float scale, int numSplits)
            throws Exception
    {

        if (numSplits == 0) {
            return;
        }

        SchemaTableName stn = new SchemaTableName(schemaName, tableName);
        String fullTableName = AccumuloClient.getFullTableName(stn);
        ZooKeeperInstance inst = new ZooKeeperInstance(conf.getInstance(), conf.getZooKeepers());
        Connector conn = inst.getConnector(conf.getUsername(), new PasswordToken(conf.getPassword()));

        AccumuloTable table = AccumuloMetadataManager.getDefault("accumulo", conf).getTable(stn);

        Type rowIdType = null;

        for (AccumuloColumnHandle ach : table.getColumns()) {
            if (ach.getName().equals(table.getRowIdName())) {
                rowIdType = ach.getType();
                break;
            }
        }

        double[] splits = getSplits(tableName, numSplits, scale);

        SortedSet<Text> tableSplits = new TreeSet<>();
        for (Double s : splits) {
            if (rowIdType == BigintType.BIGINT) {
                tableSplits.add(new Text(LexicoderRowSerializer.encode(rowIdType, s.longValue())));
            }
            else {
                throw new UnsupportedOperationException("Type " + rowIdType + " is not supported");
            }
        }

        conn.tableOperations().addSplits(fullTableName, tableSplits);

        System.out.println("Splits added, compacting");
        conn.tableOperations().compact(fullTableName, null, null, true, true);

        System.out.println("Splits are ");
        for (Text s : conn.tableOperations().listSplits(fullTableName)) {
            if (rowIdType == BigintType.BIGINT) {
                System.out.println((Long) LexicoderRowSerializer.decode(rowIdType, s.copyBytes()));
            }
            else {
                throw new UnsupportedOperationException("Type " + rowIdType + " is not supported");
            }
        }
    }

    private static double[] getSplits(String tableName, int numSplits, float scale)
    {
        long maxRowId = MAX_ROW_ID_BY_TABLE.get(tableName);

        final long endId = TABLE_SCALES.get(tableName) ? (long) (maxRowId * scale) : maxRowId;

        return Arrays.copyOfRange(linspace(1, endId, numSplits + 2), 1, numSplits + 1);
    }

    private static double[] linspace(double start, double stop, int n)
    {
        double[] values = new double[n];
        double dx = (stop - start) / (double) (n - 1);
        for (int i = 0; i < n; ++i) {
            values[i] = start + ((double) i) * dx;
        }

        return values;
    }
}
