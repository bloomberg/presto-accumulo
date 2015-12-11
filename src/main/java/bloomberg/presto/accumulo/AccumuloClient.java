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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.ColumnMetadata;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

public class AccumuloClient {
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private ZooKeeperInstance inst = null;
    private Connector conn = null;
    private AccumuloColumnMetadataProvider colMetaProvider = null;

    @Inject
    public AccumuloClient(AccumuloConfig config,
            JsonCodec<Map<String, List<AccumuloTable>>> catalogCodec)
                    throws IOException, AccumuloException,
                    AccumuloSecurityException {
        LOG.debug("constructor");
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        inst = new ZooKeeperInstance(config.getInstance(),
                config.getZooKeepers());
        conn = inst.getConnector(config.getUsername(),
                new PasswordToken(config.getPassword().getBytes()));

        colMetaProvider = AccumuloColumnMetadataProvider.getDefault(config);
    }

    public Set<String> getSchemaNames() {
        try {
            Set<String> schemas = new HashSet<>();
            schemas.add("default");

            // add all non-accumulo reserved namespaces
            for (String ns : conn.namespaceOperations().list()) {
                if (!ns.equals("accumulo")) {
                    schemas.add(ns);
                }
            }

            return schemas;
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");

        if (schema.equals("accumulo")) {
            throw new RuntimeException("accumulo is a reserved schema");
        }

        Set<String> tableNames = new HashSet<>();

        for (String accumuloTable : conn.tableOperations().list()) {
            LOG.debug(String.format("Scanned table %s from Accumulo",
                    accumuloTable));
            if (accumuloTable.contains(".")) {
                String[] tokens = accumuloTable.split("\\.");
                if (tokens.length == 2) {
                    if (tokens[0].equals(schema)) {
                        LOG.debug(String.format("Added table %s", tokens[1]));
                        tableNames.add(tokens[1]);
                    }
                } else {
                    throw new RuntimeException(String.format(
                            "Splits from %s is not of length two: %s",
                            accumuloTable, tokens));
                }
            } else if (schema.equals("default")) {
                // skip trace table
                if (!accumuloTable.equals("trace")) {
                    LOG.debug(String.format("Added table %s", accumuloTable));
                    tableNames.add(accumuloTable);
                }
            }
        }

        return tableNames;
    }

    public AccumuloTable getTable(String schema, String tableName) {
        LOG.debug("getTable");
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        return new AccumuloTable(tableName,
                colMetaProvider.getColumnMetadata(schema, tableName),
                this.getTabletSplits(schema, tableName));
    }

    public AccumuloColumn getColumnMetadata(String schema, String table,
            ColumnMetadata column) {
        return colMetaProvider.getAccumuloColumn(schema, table,
                column.getName());
    }

    public List<String> getTabletSplits(String schemaName, String tableName) {
        try {
            List<String> tabletSplits = new ArrayList<>();
            for (Text split : conn.tableOperations()
                    .listSplits(schemaName.equals("default") ? tableName
                            : schemaName + '.' + tableName)) {
                tabletSplits.add(split.toString());
            }
            return tabletSplits;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
