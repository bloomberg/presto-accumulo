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

import java.util.List;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

public abstract class AccumuloColumnMetadataProvider {

    public static final String ROW_ID_COLUMN_NAME = "recordkey";
    public static final Type ROW_ID_COLUMN_TYPE = VarcharType.VARCHAR;

    protected final String connectorId;
    protected final AccumuloConfig config;
    protected final AccumuloColumnHandle ROW_ID_COLUMN;

    public AccumuloColumnMetadataProvider(String connectorId,
            AccumuloConfig config) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.config = requireNonNull(config, "config is null");
        this.ROW_ID_COLUMN = new AccumuloColumnHandle(connectorId,
                ROW_ID_COLUMN_NAME, null, null, ROW_ID_COLUMN_TYPE, 0,
                "Accumulo row ID");
    }

    public static AccumuloColumnMetadataProvider getDefault(String connectorId,
            AccumuloConfig config) {
        return new ZooKeeperColumnMetadataProvider(connectorId, config);
    }

    public abstract List<AccumuloColumnHandle> getColumnMetadata(String schema,
            String table);

    public abstract AccumuloColumnHandle getAccumuloColumn(String schema,
            String table, String name);

    public AccumuloColumnHandle getRowIdColumn() {
        return ROW_ID_COLUMN;
    }
}
