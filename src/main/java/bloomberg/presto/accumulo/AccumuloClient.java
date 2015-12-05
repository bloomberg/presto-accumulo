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

import com.facebook.presto.spi.type.VarcharType;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

public class AccumuloClient {
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private ZooKeeperInstance inst = null;
    private Connector conn = null;

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
    }

    public Set<String> getSchemaNames() {
        try {
            Set<String> schemas = new HashSet<>();
            schemas.add("default");
            schemas.addAll(conn.namespaceOperations().list());
            return schemas;
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        Set<String> tableNames = new HashSet<>();

        for (String tableFromAccumulo : conn.tableOperations().list()) {
            LOG.debug(String.format("Scanned table %s from Accumulo",
                    tableFromAccumulo));
            if (tableFromAccumulo.contains(".")) {
                String[] tokens = tableFromAccumulo.split("\\.");
                if (tokens.length == 2) {
                    if (tokens[0].equals(schema)) {
                        LOG.debug(String.format("Added table %s", tokens[1]));
                        tableNames.add(tokens[1]);
                    }
                } else {
                    throw new RuntimeException(String.format(
                            "Splits from %s is not of length two: %s",
                            tableFromAccumulo, tokens));
                }
            } else if (schema.equals("default")) {
                LOG.debug(String.format("Added table %s", tableFromAccumulo));
                tableNames.add(tableFromAccumulo);
            }
        }

        return tableNames;
    }

    public AccumuloTable getTable(String schema, String tableName) {
        LOG.debug("getTable");
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        List<AccumuloColumn> col = new ArrayList<>();
        col.add(new AccumuloColumn("cf1", "cq1", VarcharType.VARCHAR));
        AccumuloTable table = new AccumuloTable(tableName, col);
        return table;
    }
}
