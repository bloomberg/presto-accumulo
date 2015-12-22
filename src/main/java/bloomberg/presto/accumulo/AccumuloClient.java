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
import com.facebook.presto.spi.SchemaTableName;

import bloomberg.presto.accumulo.metadata.AccumuloTableMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

public class AccumuloClient {
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private ZooKeeperInstance inst = null;
    private AccumuloConfig conf = null;
    private Connector conn = null;
    private AccumuloTableMetadataManager metaManager = null;

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

        metaManager = AccumuloTableMetadataManager
                .getDefault(connectorId.toString(), config);
    }

    public void createTable(ConnectorTableMetadata meta) {
        boolean metaOnly = (boolean) meta.getProperties()
                .get(AccumuloConnector.PROP_METADATA_ONLY);

        // Validate first column is the accumulo row id
        ColumnMetadata firstCol = meta.getColumns().get(0);
        if (!firstCol.getName()
                .equals(AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME)
                || !firstCol.getType().equals(
                        AccumuloTableMetadataManager.ROW_ID_COLUMN_TYPE)) {
            throw new InvalidParameterException(
                    String.format("First column must be '%s %s', not %s %s",
                            AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME,
                            AccumuloTableMetadataManager.ROW_ID_COLUMN_TYPE,
                            firstCol.getName(), firstCol.getType()));
        }

        if (meta.getColumns().size() == 1) {
            throw new InvalidParameterException(
                    "Must have at least one non-row ID column");
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
        columns.add(AccumuloTableMetadataManager.getRowIdColumn());

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
                meta.getTable().getTableName(), columns, (String) meta
                        .getProperties().get(AccumuloConnector.PROP_SERIALIZER));

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
                throw new RuntimeException(e);
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
            String table) {
        try {
            String fulltable = getFullTableName(schema, table);
            List<TabletSplitMetadata> tabletSplits = new ArrayList<>();
            String prevSplit = null;
            for (Text tSplit : conn.tableOperations().listSplits(fulltable)) {
                String split = tSplit.toString();

                String loc = this.getTabletLocation(fulltable, split);
                String host = HostAddress.fromString(loc).getHostText();
                int port = HostAddress.fromString(loc).getPort();

                RangeHandle rHandle = prevSplit == null
                        ? new RangeHandle(null, true, split, true)
                        : new RangeHandle(prevSplit, false, split, true);

                prevSplit = split;

                tabletSplits.add(new TabletSplitMetadata(split.toString(), host,
                        port, rHandle));
            }

            // last range from prevSplit to infinity
            String loc = this.getTabletLocation(fulltable, null);
            String host = HostAddress.fromString(loc).getHostText();
            int port = HostAddress.fromString(loc).getPort();

            tabletSplits.add(new TabletSplitMetadata(null, host, port,
                    new RangeHandle(prevSplit, false, null, true)));

            return tabletSplits;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        return location;
    }

    public static String getFullTableName(String schema, String table) {
        return schema.equals("default") ? table : schema + '.' + table;
    }

    public static String getFullTableName(SchemaTableName stName) {
        return getFullTableName(stName.getSchemaName(), stName.getTableName());
    }
}
