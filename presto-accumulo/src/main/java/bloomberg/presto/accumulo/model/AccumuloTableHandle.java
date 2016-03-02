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
package bloomberg.presto.accumulo.model;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of several Presto TableHandles for inserting data, CTAS, and a regular ol' table
 * handle. Contains table metadata!
 */
public final class AccumuloTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle, ConnectorTableHandle
{
    private final boolean internal;
    private final String connectorId;
    private final String rowId;
    private final String schema;
    private final String serializerClassName;
    private final String table;

    /**
     * Creates a new instance of {@link AccumuloTableHandle}.
     *
     * @param connectorId
     *            Presto connector ID
     * @param schema
     *            Presto schema (Accumulo namespace)
     * @param table
     *            Presto table (Accumulo table)
     * @param rowId
     *            The Presto column name that is the Accumulo row ID
     * @param internal
     *            Whether or not this table is internal, i.e. managed by Presto
     * @param serializerClassName
     *            The qualified Java class name to (de)serialize data from Accumulo
     */
    @JsonCreator
    public AccumuloTableHandle(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema, @JsonProperty("table") String table,
            @JsonProperty("rowId") String rowId, @JsonProperty("internal") boolean internal,
            @JsonProperty("serializerClassName") String serializerClassName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.internal = requireNonNull(internal, "internal is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.serializerClassName =
                requireNonNull(serializerClassName, "serializerClassName is null");
        this.table = requireNonNull(table, "table is null");
    }

    /**
     * Gets the Presto connector ID.
     *
     * @return Connector ID
     */
    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    /**
     * Gets the row ID.
     *
     * @return Row ID
     */
    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    /**
     * Gets the schema name.
     *
     * @return Schema name
     */
    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    /**
     * Gets the configured serializer class name. This method is a JsonProperty.
     *
     * @return The list of {@link AccumuloColumnHandle}
     */
    @JsonProperty
    public String getSerializerClassName()
    {
        return serializerClassName;
    }

    /**
     * Gets the table name.
     *
     * @return Table name
     */
    @JsonProperty
    public String getTable()
    {
        return table;
    }

    /**
     * Gets a Boolean value indicating if the Accumulo tables are internal, i.e. managed by Presto.
     * This method is a JsonProperty.
     *
     * @return True if internal, false otherwise
     */
    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    /**
     * Gets a new SchemaTableName for this object's schema and table
     *
     * @return new SchemaTableName
     */
    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schema, table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schema, table, rowId, internal, serializerClassName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        AccumuloTableHandle other = (AccumuloTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.schema, other.schema)
                && Objects.equals(this.table, other.table)
                && Objects.equals(this.rowId, other.rowId)
                && Objects.equals(this.internal, other.internal)
                && Objects.equals(this.serializerClassName, other.serializerClassName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("connectorId", connectorId).add("schema", schema)
                .add("table", table).add("rowId", rowId).add("internal", internal)
                .add("serializerClassName", serializerClassName).toString();
    }
}
