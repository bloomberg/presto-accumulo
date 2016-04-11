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
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.metadata.AccumuloMetadataManager;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
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
    private static LexicoderRowSerializer serializer = new LexicoderRowSerializer();

    public static void run(AccumuloConfig conf, String schemaName, String tableName, int numSplits)
            throws Exception
    {
        if (numSplits == 0) {
            return;
        }

        SchemaTableName stn = new SchemaTableName(schemaName, tableName);
        String fullTableName = AccumuloTable.getFullTableName(stn);
        String metricsTable = Indexer.getMetricsTableName(stn);

        ZooKeeperInstance inst = new ZooKeeperInstance(conf.getInstance(), conf.getZooKeepers());
        Connector conn = inst.getConnector(conf.getUsername(), new PasswordToken(conf.getPassword()));

        if (!conn.tableOperations().exists(metricsTable)) {
            throw new InvalidActivityException("Metrics table does not exist, can only split indexed tables due to need for metadata");
        }

        AccumuloTable table = AccumuloMetadataManager.getDefault(conf).getTable(stn);

        Type rowIdType = null;

        for (AccumuloColumnHandle ach : table.getColumns()) {
            if (ach.getName().equals(table.getRowId())) {
                rowIdType = ach.getType();
                break;
            }
        }

        List<byte[]> splits = getSplits(rowIdType, conn, table, conf.getUsername(), numSplits);

        SortedSet<Text> tableSplits = new TreeSet<>();
        for (byte[] s : splits) {
            tableSplits.add(new Text(s));
        }

        conn.tableOperations().addSplits(fullTableName, tableSplits);

        System.out.println("Splits added, compacting");
        conn.tableOperations().compact(fullTableName, null, null, true, true);

        System.out.println("Splits are ");
        for (Text s : conn.tableOperations().listSplits(fullTableName)) {
            System.out.println(serializer.decode(rowIdType, s.copyBytes()).toString());
        }

        Thread.sleep(60000);
    }

    private static List<byte[]> getSplits(Type rowIdType, Connector conn, AccumuloTable table, String username, int numSplits)
            throws Exception
    {
        Pair<byte[], byte[]> firstLastRow = Indexer.getMinMaxRowIds(conn, table, conn.securityOperations().getUserAuthorizations(username));

        System.out.println("Min is " + serializer.decode(rowIdType, firstLastRow.getLeft()).toString());
        System.out.println("Max is " + serializer.decode(rowIdType, firstLastRow.getRight()).toString());
        if (firstLastRow.getLeft() == null || firstLastRow.getRight() == null) {
            throw new InvalidActivityException("No data in metrics table for min and max row IDs, cannot split");
        }

        List<byte[]> splits = new ArrayList<>();
        if (rowIdType.equals(BIGINT)) {
            long min = serializer.decode(rowIdType, firstLastRow.getLeft());
            long max = serializer.decode(rowIdType, firstLastRow.getRight());

            for (Long l : linspace(min, max, numSplits + 2)) {
                splits.add(serializer.encode(BIGINT, l));
                System.out.println(l);
            }
        }
        else if (rowIdType.equals(VARCHAR)) {
            System.out.println("Splits to add are:");
            for (Long l : linspace(0, 255, numSplits + 2)) {
                String v = String.format("%02x", l);
                splits.add(serializer.encode(VARCHAR, v));
                System.out.println(v);
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

        double step = (double) (stop - start) / (double) (n - 1);

        for (double i = 0; i <= n - 2; ++i) {
            result.add((long) ((double) start + (i * step)));
        }
        result.add(stop);

        return result;
    }
}
