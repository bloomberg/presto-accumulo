/*
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
package bloomberg.presto.accumulo;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AccumuloClient
{
    private ZooKeeperInstance inst = null;
    private AccumuloConfig conf = null;
    private Connector conn = null;
    private AccumuloMetadataManager metaManager = null;

    @Inject
    public AccumuloClient(AccumuloConnectorId connectorId, AccumuloConfig config, JsonCodec<Map<String, List<AccumuloTable>>> catalogCodec)
            throws IOException, AccumuloException,
            AccumuloSecurityException
    {
        conf = requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        conn = inst.getConnector(config.getUsername(), new PasswordToken(config.getPassword().getBytes()));

        metaManager = AccumuloMetadataManager.getDefault(connectorId.toString(), config);
    }

    public AccumuloTable createTable(ConnectorTableMetadata meta)
    {
        Map<String, Object> tableProperties = meta.getProperties();
        boolean metaOnly = AccumuloTableProperties.isMetadataOnly(tableProperties);

        String rowIdColumn = AccumuloTableProperties.getRowId(tableProperties);

        if (rowIdColumn == null) {
            rowIdColumn = meta.getColumns().get(0).getName();
        }

        if (meta.getColumns().size() == 1) {
            throw new InvalidParameterException("Must have at least one non-row ID column");
        }

        // protection against map of complex types
        for (ColumnMetadata column : meta.getColumns()) {
            if (Types.isMapType(column.getType())) {
                if (Types.isMapType(Types.getKeyType(column.getType())) || Types.isMapType(Types.getValueType(column.getType())) || Types.isArrayType(Types.getKeyType(column.getType())) || Types.isArrayType(Types.getValueType(column.getType()))) {
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Key/value types of a map pairs must be plain types");
                }
            }
        }

        // parse the mapping configuration to get the accumulo fam/qual pair
        String strMapping = AccumuloTableProperties.getColumnMapping(meta.getProperties());
        if (strMapping == null || strMapping.isEmpty()) {
            throw new InvalidParameterException("Must specify column mapping property");
        }

        Map<String, Pair<String, String>> mapping = new HashMap<>();
        for (String m : strMapping.split(",")) {
            String[] tokens = m.split(":");

            if (tokens.length == 3) {
                mapping.put(tokens[0], Pair.of(tokens[1], tokens[2]));
            }
            else {
                throw new InvalidParameterException(String.format("Mapping of %s contains %d tokens instead of 3", m, tokens.length));
            }
        }

        List<String> indexedColumns = AccumuloTableProperties.getIndexColumns(tableProperties);

        // And now we parse the configured columns and create handles for the
        // metadata manager, adding the special row ID column first
        List<AccumuloColumnHandle> columns = new ArrayList<>();
        for (int i = 0; i < meta.getColumns().size(); ++i) {
            ColumnMetadata cm = meta.getColumns().get(i);
            if (cm.getName().toLowerCase().equals(rowIdColumn)) {
                columns.add(new AccumuloColumnHandle("accumulo", rowIdColumn, null, null, cm.getType(), i, "Accumulo row ID", false));
            }
            else {
                try {
                    Pair<String, String> famqual = mapping.get(cm.getName());
                    columns.add(new AccumuloColumnHandle("accumulo", cm.getName(), famqual.getLeft(), famqual.getRight(), cm.getType(), i, String.format("Accumulo column %s:%s. Indexed: %b", famqual.getLeft(), famqual.getRight(), indexedColumns.contains(cm.getName().toLowerCase())), indexedColumns.contains(cm.getName().toLowerCase())));
                }
                catch (NullPointerException e) {
                    throw new InvalidParameterException(String.format("Misconfigured mapping for presto column %s", cm.getName()));
                }
            }
        }

        boolean internal = AccumuloTableProperties.isInternal(tableProperties);
        AccumuloTable table = new AccumuloTable(meta.getTable().getSchemaName(), meta.getTable().getTableName(), columns, rowIdColumn, internal, AccumuloTableProperties.getSerializerClass(tableProperties));

        // Create dat metadata
        metaManager.createTableMetadata(table);

        if (!metaOnly) {
            try {
                if (!table.getSchemaName().equals("default") && !conn.namespaceOperations().exists(table.getSchemaName())) {
                    conn.namespaceOperations().create(table.getSchemaName());
                }

                conn.tableOperations().create(table.getFullTableName());

                if (table.isIndexed()) {
                    conn.tableOperations().create(table.getIndexTableName());

                    Map<String, Set<Text>> groups = new HashMap<>();
                    for (AccumuloColumnHandle acc : table.getColumns().stream().filter(x -> x.isIndexed()).collect(Collectors.toList())) {
                        Text indexColumnFamily = new Text(acc.getColumnFamily() + "_" + acc.getColumnQualifier());
                        groups.put(indexColumnFamily.toString(), ImmutableSet.of(indexColumnFamily));
                    }

                    conn.tableOperations().setLocalityGroups(table.getIndexTableName(), groups);
                }

            }
            catch (Exception e) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Accumulo error when creating table", e);
            }
        }

        return table;
    }

    public void dropTable(SchemaTableName stName, boolean dropTable)
    {
        String tn = getFullTableName(stName);
        metaManager.deleteTableMetadata(stName);

        if (dropTable) {
            if (conn.tableOperations().exists(tn)) {
                try {
                    conn.tableOperations().delete(tn);

                    String indexTable = getIndexTableName(stName);
                    if (conn.tableOperations().exists(indexTable)) {
                        conn.tableOperations().delete(indexTable);
                    }

                }
                catch (Exception e) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to delete table");
                }
            }
        }
    }

    public Set<String> getSchemaNames()
    {
        return metaManager.getSchemaNames();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return metaManager.getTableNames(schema);
    }

    public AccumuloTable getTable(SchemaTableName table)
    {
        requireNonNull(table, "schema table name is null");
        return metaManager.getTable(table);
    }

    public List<TabletSplitMetadata> getTabletSplits(ConnectorSession session, String schema, String table, Domain rDom, List<AccumuloColumnConstraint> constraints)
    {
        try {
            String tableName = getFullTableName(schema, table);
            String indexTable = getIndexTableName(schema, table);

            // get the range based on predicate pushdown
            final Collection<Range> predicatePushdownRanges;
            if (AccumuloSessionProperties.isOptimizeRangePredicatePushdownEnabled(session)) {
                predicatePushdownRanges = getPredicatePushdownRanges(tableName, rDom);
            }
            else {
                predicatePushdownRanges = new HashSet<>();
                predicatePushdownRanges.add(new Range());
            }

            // Get ranges from any secondary index tables, ensuring the ranges
            // via the index are within at least one predicate pushdown range
            final Collection<Range> columnPushdownRanges;
            if (AccumuloSessionProperties.isSecondaryIndexEnabled(session)) {
                columnPushdownRanges = applySecondaryIndex(tableName, indexTable, constraints, predicatePushdownRanges);
            }
            else {
                columnPushdownRanges = new HashSet<>();
            }

            // choose the set of ranges to use, the finer-grained ranges from
            // the result of the secondary index, or the original ones
            final Collection<Range> finalRanges;
            if (columnPushdownRanges.size() > 0) {
                finalRanges = columnPushdownRanges;
            }
            else {
                finalRanges = predicatePushdownRanges;
            }

            // Split the ranges on tablet boundaries
            final Collection<Range> splitRanges;
            if (AccumuloSessionProperties.isOptimizeRangeSplitsEnabled(session)) {
                splitRanges = splitByTabletBoundaries(tableName, finalRanges);
            }
            else {
                splitRanges = finalRanges;
            }

            // and now bin all the ranges into presto splits
            return binRanges(splitRanges);
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to get splits", e);
        }
    }

    private Collection<Range> getPredicatePushdownRanges(String fulltable, Domain rDom)
            throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException
    {
        Set<Range> ranges = new HashSet<>();
        // if we have no predicate pushdown, use the full range
        if (rDom == null) {
            ranges.add(new Range());
        }
        else {
            // else, add the ranges based on the predicate pushdown
            for (com.facebook.presto.spi.predicate.Range r : rDom.getValues().getRanges().getOrderedRanges()) {
                ranges.add(getRangeFromPrestoRange(fulltable, r));
            }
        }
        return ranges;
    }

    private Collection<Range> applySecondaryIndex(String fulltable, String indexTable, Collection<AccumuloColumnConstraint> constraints, Collection<Range> predicatePushdownRanges)
            throws AccumuloException,
            AccumuloSecurityException, TableNotFoundException
    {
        Set<Range> columnPushdownRanges = new HashSet<>();
        List<Range> indexRanges = new ArrayList<>();
        Authorizations indexScanAuths = conn.securityOperations().getUserAuthorizations(conf.getUsername());
        Text cq = new Text();
        for (AccumuloColumnConstraint acc : constraints.stream().filter(x -> x.isIndexed()).collect(Collectors.toList())) {
            for (com.facebook.presto.spi.predicate.Range r : acc.getDomain().getValues().getRanges().getOrderedRanges()) {
                indexRanges.add(getRangeFromPrestoRange(fulltable, r));
            }

            BatchScanner scan = conn.createBatchScanner(indexTable, indexScanAuths, 10);
            scan.setRanges(indexRanges);
            scan.fetchColumnFamily(new Text(acc.getFamily() + "_" + acc.getQualifier()));
            for (Entry<Key, Value> entry : scan) {
                entry.getKey().getColumnQualifier(cq);

                if (inRange(cq, predicatePushdownRanges)) {
                    columnPushdownRanges.add(new Range(cq));
                }
            }
            scan.close();
        }
        return columnPushdownRanges;
    }

    private Collection<Range> splitByTabletBoundaries(String tableName, Collection<Range> ranges)
            throws TableNotFoundException,
            AccumuloException, AccumuloSecurityException
    {
        Set<Range> splitRanges = new HashSet<>();
        for (Range r : ranges) {
            splitRanges.addAll(conn.tableOperations().splitRangeByTablets(tableName, r, Integer.MAX_VALUE));
        }
        return splitRanges;
    }

    private List<TabletSplitMetadata> binRanges(Collection<Range> splitRanges)
    {
        List<TabletSplitMetadata> prestoSplits = new ArrayList<>();

        // convert Ranges to RangeHandles
        List<RangeHandle> rHandles = new ArrayList<>();
        splitRanges.stream().forEach(x -> rHandles.add(RangeHandle.from(x)));

        // Bin them together into splits
        Collections.shuffle(rHandles);

        String loc = "localhost:9997";
        int toAdd = rHandles.size();
        int fromIndex = 0;
        int toIndex = Math.min(toAdd, 1000);
        do {
            prestoSplits.add(new TabletSplitMetadata(loc, rHandles.subList(fromIndex, toIndex)));
            toAdd -= toIndex - fromIndex;
            fromIndex = toIndex;
            toIndex += Math.min(toAdd, 1000);
        }
        while (toAdd > 0);
        return prestoSplits;
    }

    private boolean inRange(Text cq, Collection<Range> preSplitRanges)
    {
        Key kCq = new Key(cq);
        for (Range r : preSplitRanges) {
            if (!r.beforeStartKey(kCq) && !r.afterEndKey(kCq)) {
                return true;
            }
        }
        return false;
    }

    private Range getRangeFromPrestoRange(String fulltable, com.facebook.presto.spi.predicate.Range pRange)
            throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException
    {
        final Range preSplitRange;
        if (pRange.isAll()) {
            preSplitRange = new Range();
        }
        else if (pRange.isSingleValue()) {
            Text split = new Text(LexicoderRowSerializer.encode(pRange.getType(), pRange.getSingleValue()));
            preSplitRange = new Range(split);
        }
        else {
            if (pRange.getLow().isLowerUnbounded()) {
                boolean inclusive = pRange.getHigh().getBound() == Bound.EXACTLY;
                Text split = new Text(LexicoderRowSerializer.encode(pRange.getType(), pRange.getHigh().getValue()));
                preSplitRange = new Range(null, false, split, inclusive);
            }
            else if (pRange.getHigh().isUpperUnbounded()) {
                boolean inclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                Text split = new Text(LexicoderRowSerializer.encode(pRange.getType(), pRange.getLow().getValue()));
                preSplitRange = new Range(split, inclusive, null, false);
            }
            else {
                boolean startKeyInclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                Text startSplit = new Text(LexicoderRowSerializer.encode(pRange.getType(), pRange.getLow().getValue()));

                boolean endKeyInclusive = pRange.getHigh().getBound() == Bound.EXACTLY;
                Text endSplit = new Text(LexicoderRowSerializer.encode(pRange.getType(), pRange.getHigh().getValue()));
                preSplitRange = new Range(startSplit, startKeyInclusive, endSplit, endKeyInclusive);
            }
        }

        return preSplitRange;
    }

    public String getTabletLocation(String fulltable, byte[] key)
    {
        try {
            if (key != null) {
                String tableId = conn.tableOperations().tableIdMap().get(fulltable);
                Scanner scan = conn.createScanner("accumulo.metadata", conn.securityOperations().getUserAuthorizations(conf.getUsername()));

                scan.fetchColumnFamily(new Text("loc"));

                Key defaultTabletRow = new Key(tableId + '<');
                Text splitCompareKey = new Text();
                Text scannedCompareKey = new Text();

                splitCompareKey.set(key, 0, key.length - 1);

                Key start = new Key(tableId);
                Key end = defaultTabletRow.followingKey(PartialKey.ROW);
                scan.setRange(new Range(start, end));

                String location = null;
                for (Entry<Key, Value> kvp : scan) {
                    byte[] keyBytes = kvp.getKey().getRow().copyBytes();
                    scannedCompareKey.set(keyBytes, 3, keyBytes.length - 3);
                    if (scannedCompareKey.getLength() > 0) {
                        int compareTo = splitCompareKey.compareTo(scannedCompareKey);
                        if (compareTo <= 0) {
                            location = kvp.getValue().toString();
                        }
                    }
                }
                scan.close();

                if (location != null) {
                    return location;
                }
            }

            return getDefaultTabletLocation(fulltable);
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to get tablet location");
        }
    }

    public String getDefaultTabletLocation(String fulltable)
    {
        try {
            String tableId = conn.tableOperations().tableIdMap().get(fulltable);
            Scanner scan = conn.createScanner("accumulo.metadata", conn.securityOperations().getUserAuthorizations(conf.getUsername()));

            scan.fetchColumnFamily(new Text("loc"));
            scan.setRange(new Range(tableId + '<'));

            String location = null;
            for (Entry<Key, Value> kvp : scan) {
                assert location == null;
                location = kvp.getValue().toString();
            }
            scan.close();

            return location;
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Failed to get tablet location");
        }
    }

    public static String getFullTableName(String schema, String table)
    {
        return schema.equals("default") ? table : schema + '.' + table;
    }

    public static String getFullTableName(SchemaTableName stName)
    {
        return getFullTableName(stName.getSchemaName(), stName.getTableName());
    }

    public static String getIndexTableName(String schema, String table)
    {
        return schema.equals("default") ? table + "_idx" : schema + '.' + table + "_idx";
    }

    public static String getIndexTableName(SchemaTableName stName)
    {
        return getIndexTableName(stName.getSchemaName(), stName.getTableName());
    }
}
