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
import io.airlift.json.JsonCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class AccumuloClient {
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, AccumuloTable>>> schemas;
    private String schema = null;
    private String table = null;

    @Inject
    public AccumuloClient(AccumuloConfig config,
            JsonCodec<Map<String, List<AccumuloTable>>> catalogCodec)
                    throws IOException {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        schema = config.getSchema();
        table = config.getTable();

        schemas = Suppliers.memoize(schemasSupplier(catalogCodec));
    }

    public Set<String> getSchemaNames() {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        Map<String, AccumuloTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public AccumuloTable getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, AccumuloTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private Supplier<Map<String, Map<String, AccumuloTable>>> schemasSupplier(
            final JsonCodec<Map<String, List<AccumuloTable>>> catalogCodec) {
        Map<String, Map<String, AccumuloTable>> tables = new HashMap<>();
        List<AccumuloColumn> col = new ArrayList<>();
        col.add(new AccumuloColumn("cf1", "cq1", VarcharType.VARCHAR));
        Map<String, AccumuloTable> value = new HashMap<>();
        value.put(table, new AccumuloTable(table, col));
        tables.put(schema, value);
        return () -> {
            return ImmutableMap.copyOf(tables);
        };
    }
}
