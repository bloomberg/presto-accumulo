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
package bloomberg.presto.accumulo.metadata;

import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloTable;
import com.facebook.presto.spi.SchemaTableName;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class AccumuloMetadataManager
{
    protected final String connectorId;
    protected final AccumuloConfig config;

    public AccumuloMetadataManager(String connectorId, AccumuloConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.config = requireNonNull(config, "config is null");
    }

    public static AccumuloMetadataManager getDefault(String connectorId, AccumuloConfig config)
    {
        return new ZooKeeperMetadataManager(connectorId, config);
    }

    public abstract Set<String> getSchemaNames();

    public abstract Set<String> getTableNames(String schema);

    public abstract AccumuloTable getTable(SchemaTableName table);

    public abstract void createTableMetadata(AccumuloTable table);

    public abstract void deleteTableMetadata(SchemaTableName stName);
}
