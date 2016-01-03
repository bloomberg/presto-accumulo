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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

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

    public void createTable(ConnectorTableMetadata meta) {
        boolean metaOnly = (boolean) meta.getProperties()
                .get(AccumuloConnector.PROP_METADATA_ONLY);

        // Validate first column is the accumulo row id
        ColumnMetadata firstCol = meta.getColumns().get(0);
        if (!firstCol.getName()
                .equals(AccumuloMetadataManager.ROW_ID_COLUMN_NAME)
                || !firstCol.getType()
                        .equals(AccumuloMetadataManager.ROW_ID_COLUMN_TYPE)) {
            throw new InvalidParameterException(
                    String.format("First column must be '%s %s', not %s %s",
                            AccumuloMetadataManager.ROW_ID_COLUMN_NAME,
                            AccumuloMetadataManager.ROW_ID_COLUMN_TYPE,
                            firstCol.getName(), firstCol.getType()));
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
        String strMapping = (String) meta.getProperties()
                .get(AccumuloConnector.PROP_COLUMN_MAPPING);
        if (strMapping == null || strMapping.isEmpty()) {
            throw new InvalidParameterException(String.format(
                    "Must specify mapping property in WITH (%s = ...)",
                    AccumuloConnector.PROP_COLUMN_MAPPING));
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

        // And now we parse the configured columns and create handles for the
        // metadata manager, adding the special row ID column first
        List<AccumuloColumnHandle> columns = new ArrayList<>();
        columns.add(AccumuloMetadataManager.getRowIdColumn());

        for (int i = 1; i < meta.getColumns().size(); ++i) {
            ColumnMetadata cm = meta.getColumns().get(i);
            try {
                Pair<String, String> famqual = mapping.get(cm.getName());
                columns.add(new AccumuloColumnHandle("accumulo", cm.getName(),
                        famqual.getLeft(), famqual.getRight(), cm.getType(), i,
                        String.format("Accumulo column %s:%s",
                                famqual.getLeft(), famqual.getRight())));
            } catch (NullPointerException e) {
                throw new InvalidParameterException(String.format(
                        "Misconfigured mapping for presto column %s",
                        cm.getName()));
            }
        }

        AccumuloTable table = new AccumuloTable(meta.getTable().getSchemaName(),
                meta.getTable().getTableName(), columns,
                (String) meta.getProperties()
                        .get(AccumuloConnector.PROP_SERIALIZER));

        // Create dat metadata
        metaManager.createTableMetadata(table);

        if (!metaOnly) {
            try {
                if (meta.getTable().getSchemaName().equals("default")) {
                    conn.tableOperations()
                            .create(meta.getTable().getTableName());
                } else {
                    if (!conn.namespaceOperations()
                            .exists(meta.getTable().getSchemaName())) {
                        conn.namespaceOperations()
                                .create(meta.getTable().getSchemaName());
                    }

                    conn.tableOperations().create(meta.getTable().toString());
                }
            } catch (Exception e) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Accumulo error when creating table", e);
            }
        }
    }

    public void dropTable(SchemaTableName stName) {
        metaManager.deleteTableMetadata(stName);
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

        Range preSplitRange;
        if (pRange.isAll()) {
            preSplitRange = new Range();
        } else if (pRange.isSingleValue()) {
            preSplitRange = new Range(
                    ((Slice) pRange.getSingleValue()).toStringUtf8());
        } else {
            if (pRange.getLow().isLowerUnbounded()) {
                boolean inclusive = pRange.getHigh()
                        .getBound() == Bound.EXACTLY;
                String split = ((Slice) pRange.getHigh().getValue())
                        .toStringUtf8();
                preSplitRange = new Range(null, false, split, inclusive);
            } else if (pRange.getHigh().isUpperUnbounded()) {
                boolean inclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                String split = ((Slice) pRange.getLow().getValue())
                        .toStringUtf8();
                preSplitRange = new Range(split, inclusive, null, false);
            } else {
                boolean startKeyInclusive = pRange.getLow()
                        .getBound() == Bound.EXACTLY;
                String startSplit = ((Slice) pRange.getLow().getValue())
                        .toStringUtf8();

                boolean endKeyInclusive = pRange.getHigh()
                        .getBound() == Bound.EXACTLY;
                String endSplit = ((Slice) pRange.getHigh().getValue())
                        .toStringUtf8();
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

        String startKey = range.getStartKey().getRow().toString();
        String endKey = range.getEndKey().getRow().toString();

        RangeHandle rHandle = new RangeHandle(startKey,
                range.isStartKeyInclusive(), endKey, range.isEndKeyInclusive());
        String loc = this.getTabletLocation(fulltable, endKey);
        String host = HostAddress.fromString(loc).getHostText();
        int port = HostAddress.fromString(loc).getPort();
        return new TabletSplitMetadata(endKey, host, port, rHandle);
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
    public String getTabletLocation(String fulltable, String split)
            throws TableNotFoundException, AccumuloException,
            AccumuloSecurityException {
        String tableId = conn.tableOperations().tableIdMap().get(fulltable);
        Scanner scan = conn.createScanner("accumulo.metadata",
                conn.securityOperations()
                        .getUserAuthorizations(conf.getUsername()));

        if (split != null) {
            scan.setRange(new Range(tableId + ';' + split));
        } else {
            scan.setRange(new Range(tableId + '<'));
        }

        scan.fetchColumnFamily(new Text("loc"));

        String location = null;
        for (Entry<Key, Value> kvp : scan) {
            assert location == null;
            location = kvp.getValue().toString();
        }

        LOG.debug(String.format("Location of split %s for table %s is %s",
                split, fulltable, location));
        if (location == null) {
            // TODO need to locate a split based on what tablet it would be in,
            // then find that tablet location
            return "localhost:9997";
        } else {
            return location;
        }
    }

    public static String getFullTableName(String schema, String table) {
        return schema.equals("default") ? table : schema + '.' + table;
    }

    public static String getFullTableName(SchemaTableName stName) {
        return getFullTableName(stName.getSchemaName(), stName.getTableName());
    }
}
