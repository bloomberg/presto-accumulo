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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.LexicoderRowSerializer;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

public class AccumuloClient {
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private ZooKeeperInstance inst = null;
    private AccumuloConfig conf = null;
    private Connector conn = null;
    private AccumuloMetadataManager metaManager = null;

    @Inject
    public AccumuloClient(AccumuloConnectorId connectorId,
            AccumuloConfig config,
            JsonCodec<Map<String, List<AccumuloTable>>> catalogCodec)
                    throws IOException, AccumuloException,
                    AccumuloSecurityException {
        conf = requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        inst = new ZooKeeperInstance(config.getInstance(),
                config.getZooKeepers());
        conn = inst.getConnector(config.getUsername(),
                new PasswordToken(config.getPassword().getBytes()));

        metaManager = AccumuloMetadataManager.getDefault(connectorId.toString(),
                config);
    }

    public AccumuloTable createTable(ConnectorTableMetadata meta) {
        Map<String, Object> tableProperties = meta.getProperties();
        boolean metaOnly = AccumuloTableProperties
                .isMetadataOnly(tableProperties);

        String rowIdColumn = AccumuloTableProperties.getRowId(tableProperties);

        if (rowIdColumn == null) {
            rowIdColumn = meta.getColumns().get(0).getName();
        }

        if (meta.getColumns().size() == 1) {
            throw new InvalidParameterException(
                    "Must have at least one non-row ID column");
        }

        // protection against map of complex types
        for (ColumnMetadata column : meta.getColumns()) {
            if (Types.isMapType(column.getType())) {
                if (Types.isMapType(Types.getKeyType(column.getType()))
                        || Types.isMapType(Types.getValueType(column.getType()))
                        || Types.isArrayType(Types.getKeyType(column.getType()))
                        || Types.isArrayType(
                                Types.getValueType(column.getType()))) {
                    throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                            "Key/value types of a map pairs must be plain types");
                }
            }
        }

        // parse the mapping configuration to get the accumulo fam/qual pair
        String strMapping = AccumuloTableProperties
                .getColumnMapping(meta.getProperties());
        if (strMapping == null || strMapping.isEmpty()) {
            throw new InvalidParameterException(
                    "Must specify column mapping property");
        }

        Map<String, Pair<String, String>> mapping = new HashMap<>();
        for (String m : strMapping.split(",")) {
            String[] tokens = m.split(":");

            if (tokens.length == 3) {
                mapping.put(tokens[0], Pair.of(tokens[1], tokens[2]));
            } else {
                throw new InvalidParameterException(String.format(
                        "Mapping of %s contains %d tokens instead of 3", m,
                        tokens.length));
            }
        }

        List<String> indexedColumns = AccumuloTableProperties
                .getIndexColumns(tableProperties);

        // And now we parse the configured columns and create handles for the
        // metadata manager, adding the special row ID column first
        List<AccumuloColumnHandle> columns = new ArrayList<>();
        for (int i = 0; i < meta.getColumns().size(); ++i) {

            ColumnMetadata cm = meta.getColumns().get(i);
            if (cm.getName().toLowerCase().equals(rowIdColumn)) {
                columns.add(new AccumuloColumnHandle("accumulo", rowIdColumn,
                        null, null, cm.getType(), i, "Accumulo row ID", false));
            } else {
                try {
                    Pair<String, String> famqual = mapping.get(cm.getName());
                    columns.add(new AccumuloColumnHandle("accumulo",
                            cm.getName(), famqual.getLeft(), famqual.getRight(),
                            cm.getType(), i,
                            String.format("Accumulo column %s:%s. Indexed: %b",
                                    famqual.getLeft(), famqual.getRight(),
                                    indexedColumns.contains(
                                            cm.getName().toLowerCase())),
                            indexedColumns
                                    .contains(cm.getName().toLowerCase())));
                } catch (NullPointerException e) {
                    throw new InvalidParameterException(String.format(
                            "Misconfigured mapping for presto column %s",
                            cm.getName()));
                }
            }
        }

        boolean internal = AccumuloTableProperties.isInternal(tableProperties);
        AccumuloTable table = new AccumuloTable(meta.getTable().getSchemaName(),
                meta.getTable().getTableName(), columns, rowIdColumn, internal,
                AccumuloTableProperties.getSerializerClass(tableProperties));

        // Create dat metadata
        metaManager.createTableMetadata(table);

        if (!metaOnly) {
            try {
                if (!table.getSchemaName().equals("default") && !conn
                        .namespaceOperations().exists(table.getSchemaName())) {
                    LOG.debug("Creating namespace %s", table.getSchemaName());
                    conn.namespaceOperations().create(table.getSchemaName());
                }

                LOG.debug("Creating table %s", table.getFullTableName());
                conn.tableOperations().create(table.getFullTableName());

                if (table.isIndexed()) {
                    LOG.debug("Creating index table %s",
                            table.getIndexTableName());
                    conn.tableOperations().create(table.getIndexTableName());
                } else {
                    LOG.debug("Table %s is not indexed");
                }

            } catch (Exception e) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Accumulo error when creating table", e);
            }
        }

        return table;
    }

    public void dropTable(SchemaTableName stName, boolean dropTable) {
        String tn = getFullTableName(stName);
        metaManager.deleteTableMetadata(stName);

        if (dropTable) {
            if (conn.tableOperations().exists(tn)) {
                try {
                    conn.tableOperations().delete(tn);
                } catch (Exception e) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                            "Failed to delete table");
                }
            }
        }
    }

    public Set<String> getSchemaNames() {
        return metaManager.getSchemaNames();
    }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        return metaManager.getTableNames(schema);
    }

    public AccumuloTable getTable(SchemaTableName table) {
        requireNonNull(table, "schema table name is null");
        return metaManager.getTable(table);
    }

    public List<TabletSplitMetadata> getTabletSplits(String schema,
            String table, Domain rDom) {
        try {
            String fulltable = getFullTableName(schema, table);
            List<TabletSplitMetadata> tabletSplits = new ArrayList<>();
            if (rDom != null) {
                Logger.get(getClass()).debug("COLUMN recordkey HAS %d RANGES",
                        rDom.getValues().getRanges().getRangeCount());
                for (com.facebook.presto.spi.predicate.Range r : rDom
                        .getValues().getRanges().getOrderedRanges()) {
                    tabletSplits.addAll(getTabletsFromRange(fulltable, r));
                }
            } else {
                for (Range r : conn.tableOperations().splitRangeByTablets(
                        fulltable, new Range(), Integer.MAX_VALUE)) {
                    tabletSplits.add(getTableSplitMetadata(fulltable, r));
                }
            }
            return tabletSplits;
        } catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Failed to get splits", e);
        }
    }

    private Collection<? extends TabletSplitMetadata> getTabletsFromRange(
            String fulltable, com.facebook.presto.spi.predicate.Range pRange)
                    throws AccumuloException, AccumuloSecurityException,
                    TableNotFoundException {

        List<TabletSplitMetadata> tabletSplits = new ArrayList<>();

        final Range preSplitRange;
        if (pRange.isAll()) {
            LOG.debug("SPLITS ARE ALL");
            preSplitRange = new Range();
        } else if (pRange.isSingleValue()) {
            LOG.debug("SINGLE SPLIT IS %s", pRange.getSingleValue());
            Text split = new Text(LexicoderRowSerializer
                    .encode(pRange.getType(), pRange.getSingleValue()));
            preSplitRange = new Range(split);
        } else {
            if (pRange.getLow().isLowerUnbounded()) {
                LOG.debug("SPLITS ARE %s and %s", null,
                        pRange.getHigh().getValue());
                boolean inclusive = pRange.getHigh()
                        .getBound() == Bound.EXACTLY;
                Text split = new Text(LexicoderRowSerializer
                        .encode(pRange.getType(), pRange.getHigh().getValue()));
                preSplitRange = new Range(null, false, split, inclusive);
            } else if (pRange.getHigh().isUpperUnbounded()) {
                LOG.debug("SPLITS ARE %s and %s", pRange.getLow().getValue(),
                        null);
                boolean inclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                Text split = new Text(LexicoderRowSerializer
                        .encode(pRange.getType(), pRange.getLow().getValue()));
                preSplitRange = new Range(split, inclusive, null, false);
            } else {
                LOG.debug("SPLITS ARE %s and %s", pRange.getLow().getValue(),
                        pRange.getHigh().getValue());
                boolean startKeyInclusive = pRange.getLow()
                        .getBound() == Bound.EXACTLY;
                Text startSplit = new Text(LexicoderRowSerializer
                        .encode(pRange.getType(), pRange.getLow().getValue()));

                boolean endKeyInclusive = pRange.getHigh()
                        .getBound() == Bound.EXACTLY;
                Text endSplit = new Text(LexicoderRowSerializer
                        .encode(pRange.getType(), pRange.getHigh().getValue()));
                preSplitRange = new Range(startSplit, startKeyInclusive,
                        endSplit, endKeyInclusive);
            }
        }

        for (Range r : conn.tableOperations().splitRangeByTablets(fulltable,
                preSplitRange, Integer.MAX_VALUE)) {
            tabletSplits.add(getTableSplitMetadata(fulltable, r));
        }

        return tabletSplits;
    }

    private TabletSplitMetadata getTableSplitMetadata(String fulltable,
            Range range) throws TableNotFoundException, AccumuloException,
                    AccumuloSecurityException {

        byte[] startKey = range.getStartKey() != null
                ? range.getStartKey().getRow().copyBytes() : null;
        byte[] endKey = range.getEndKey() != null
                ? range.getEndKey().getRow().copyBytes() : null;

        RangeHandle rHandle = new RangeHandle(startKey,
                range.isStartKeyInclusive(), endKey, range.isEndKeyInclusive());
        String loc = this.getTabletLocation(fulltable, endKey);
        return new TabletSplitMetadata(endKey, loc, rHandle);
    }

    /**
     * Scans Accumulo's metadata table to retrieve the
     * 
     * @param fulltable
     *            The full table name &lt;namespace&gt;.&lt;tablename&gt;, or
     *            &lt;tablename&gt; if default namespace
     * @param split
     *            The split (end-row), or null for the default split (last split
     *            in the sequence)
     * @return The hostname:port pair where the split is located
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public String getTabletLocation(String fulltable, byte[] key) {
        try {
            if (key != null) {
                String tableId = conn.tableOperations().tableIdMap()
                        .get(fulltable);
                Scanner scan = conn.createScanner("accumulo.metadata",
                        conn.securityOperations()
                                .getUserAuthorizations(conf.getUsername()));

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
                        int compareTo = splitCompareKey
                                .compareTo(scannedCompareKey);
                        if (compareTo <= 0) {
                            location = kvp.getValue().toString();
                        }
                    }
                }
                scan.close();

                if (location != null) {
                    LOG.debug(String.format(
                            "Location of split %s for table %s is %s", location,
                            fulltable, location));
                    return location;
                }
            }

            return getDefaultTabletLocation(fulltable);
        } catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Failed to get tablet location");
        }
    }

    public String getDefaultTabletLocation(String fulltable) {
        try {
            String tableId = conn.tableOperations().tableIdMap().get(fulltable);
            Scanner scan = conn.createScanner("accumulo.metadata",
                    conn.securityOperations()
                            .getUserAuthorizations(conf.getUsername()));

            scan.fetchColumnFamily(new Text("loc"));
            scan.setRange(new Range(tableId + '<'));

            String location = null;
            for (Entry<Key, Value> kvp : scan) {
                assert location == null;
                location = kvp.getValue().toString();
            }
            scan.close();

            LOG.debug(String.format(
                    "Location of default table for table %s is %s", fulltable,
                    location));
            return location;
        } catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Failed to get tablet location");
        }
    }

    public static String getFullTableName(String schema, String table) {
        return schema.equals("default") ? table : schema + '.' + table;
    }

    public static String getFullTableName(SchemaTableName stName) {
        return getFullTableName(stName.getSchemaName(), stName.getTableName());
    }
}
