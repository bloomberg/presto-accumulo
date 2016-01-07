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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class AccumuloTableHandle implements ConnectorInsertTableHandle,
        ConnectorOutputTableHandle, ConnectorTableHandle {
    private final boolean internal;
    private final String connectorId;
    private final String schemaName;
    private final String serializerClassName;
    private final String tableName;

    @JsonCreator
    public AccumuloTableHandle(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("internal") boolean internal,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("serializerClassName") String serializerClassName) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.internal = requireNonNull(internal, "internal is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.serializerClassName = requireNonNull(serializerClassName,
                "serializerClassName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getSerializerClassName() {
        return serializerClassName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public boolean isInternal() {
        return internal;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, schemaName, tableName,
                serializerClassName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        AccumuloTableHandle other = (AccumuloTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.serializerClassName,
                        other.serializerClassName);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("connectorId", connectorId)
                .add("schemaName", schemaName).add("tableName", tableName)
                .add("serializerClassName", serializerClassName).toString();
    }
}
