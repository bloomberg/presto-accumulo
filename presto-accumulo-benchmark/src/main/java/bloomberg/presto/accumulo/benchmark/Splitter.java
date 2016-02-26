package bloomberg.presto.accumulo.benchmark;

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.index.Indexer;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.metadata.AccumuloTable;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.activity.InvalidActivityException;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class Splitter
{
    public static void run(AccumuloConfig conf, String schemaName, String tableName, int numSplits)
            throws Exception
    {
        if (numSplits == 0) {
            return;
        }

        SchemaTableName stn = new SchemaTableName(schemaName, tableName);
        String fullTableName = AccumuloClient.getFullTableName(stn);
        String metricsTable = Indexer.getMetricsTableName(stn);

        ZooKeeperInstance inst = new ZooKeeperInstance(conf.getInstance(), conf.getZooKeepers());
        Connector conn = inst.getConnector(conf.getUsername(), new PasswordToken(conf.getPassword()));

        if (!conn.tableOperations().exists(metricsTable)) {
            throw new InvalidActivityException("Metrics table does not exist, can only split indexed tables due to need for metadata");
        }

        AccumuloTable table = AccumuloMetadataManager.getDefault("accumulo", conf).getTable(stn);

        Type rowIdType = null;

        for (AccumuloColumnHandle ach : table.getColumns()) {
            if (ach.getName().equals(table.getRowId())) {
                rowIdType = ach.getType();
                break;
            }
        }

        List<byte[]> splits = getSplits(rowIdType, conn, metricsTable, conf.getUsername(), numSplits);

        SortedSet<Text> tableSplits = new TreeSet<>();
        for (byte[] s : splits) {
            tableSplits.add(new Text(s));
        }

        conn.tableOperations().addSplits(fullTableName, tableSplits);

        System.out.println("Splits added, compacting");
        conn.tableOperations().compact(fullTableName, null, null, true, true);

        System.out.println("Splits are ");
        for (Text s : conn.tableOperations().listSplits(fullTableName)) {
            System.out.println(LexicoderRowSerializer.decode(rowIdType, s.copyBytes()).toString());
        }
    }

    private static List<byte[]> getSplits(Type rowIdType, Connector conn, String metricsTable, String username, int numSplits)
            throws Exception
    {
        Pair<byte[], byte[]> firstLastRow = Indexer.getMinMaxRowIds(conn, metricsTable, username);

        System.out.println("Min is " + LexicoderRowSerializer.decode(rowIdType, firstLastRow.getLeft()).toString());
        System.out.println("Max is " + LexicoderRowSerializer.decode(rowIdType, firstLastRow.getRight()).toString());
        if (firstLastRow.getLeft() == null || firstLastRow.getRight() == null) {
            throw new InvalidActivityException("No data in metrics table for min and max row IDs, cannot split");
        }

        List<byte[]> splits = new ArrayList<>();
        if (rowIdType.equals(BIGINT)) {
            long min = LexicoderRowSerializer.decode(rowIdType, firstLastRow.getLeft());
            long max = LexicoderRowSerializer.decode(rowIdType, firstLastRow.getRight());

            for (Long l : linspace(min, max, numSplits)) {
                splits.add(LexicoderRowSerializer.encode(BIGINT, l));
            }
        }
        else if (rowIdType.equals(VARCHAR)) {
            for (Long l : linspace(0, 255, numSplits + 2)) {
                String v = String.format("%02x", l);
                splits.add(LexicoderRowSerializer.encode(VARCHAR, v));
            }
        }
        else {
            throw new UnsupportedOperationException("Can only split BIGINT and VARCHAR columns");
        }

        splits.remove(0);
        splits.remove(splits.size() - 1);

        return splits;
    }

    public static List<Long> linspace(long start, long stop, int n)
    {
        List<Long> result = new ArrayList<>();

        long step = (stop - start) / (n - 1);

        for (int i = 0; i <= n - 2; i++) {
            result.add(start + (i * step));
        }
        result.add(stop);

        return result;
    }
}
