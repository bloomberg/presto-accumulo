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

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.index.Indexer;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates metadata regarding an Accumulo table in Presto. It is a jackson
 * serializable object.
 */
public class AccumuloTable
{
    private final boolean indexed;
    private final boolean internal;
    private final String rowId;
    private final String schema;
    private final String table;
    private final List<AccumuloColumnHandle> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final String serializerClassName;

    /***
     * Creates a new instance of AccumuloTable
     *
     * @param schema
     *            Presto schema (Accumulo namespace)
     * @param table
     *            Presto table (Accumulo table)
     * @param columns
     *            A list of {@link AccumuloColumnHandle} objects for the table
     * @param rowId
     *            The Presto column name that is the Accumulo row ID
     * @param internal
     *            Whether or not this table is internal, i.e. managed by Presto
     * @param serializerClassName
     *            The qualified Java class name to (de)serialize data from Accumulo
     */
    @JsonCreator
    public AccumuloTable(@JsonProperty("schema") String schema, @JsonProperty("table") String table,
            @JsonProperty("columns") List<AccumuloColumnHandle> columns,
            @JsonProperty("rowId") String rowId, @JsonProperty("internal") boolean internal,
            @JsonProperty("serializerClassName") String serializerClassName)
    {
        this.internal = requireNonNull(internal, "internal is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns are null"));
        this.serializerClassName =
                requireNonNull(serializerClassName, "serializerClassName is null");

        boolean indexed = false;
        // Extract the ColumnMetadata from the handles for faster access
        ImmutableList.Builder<ColumnMetadata> cmb = ImmutableList.builder();
        for (AccumuloColumnHandle column : this.columns) {
            cmb.add(column.getColumnMetadata());
            indexed = indexed || column.isIndexed();
        }

        this.indexed = indexed;
        this.columnsMetadata = cmb.build();
    }

    /**
     * Gets the row ID. This method is JsonProperty.
     *
     * @return Row ID
     */
    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    /**
     * Gets the schema name. This method is JsonProperty.
     *
     * @return Schema name
     */
    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    /**
     * Gets the table name. This method is JsonProperty.
     *
     * @return Table name
     */
    @JsonProperty
    public String getTable()
    {
        return table;
    }

    /**
     * Gets the full name of the index table. This method is ignored by jackson.
     *
     * @see Indexer#getIndexTableName
     * @return Index table name
     */
    @JsonIgnore
    public String getIndexTableName()
    {
        return Indexer.getIndexTableName(schema, table);
    }

    /**
     * Gets the full name of the metrics table. This method is ignored by jackson.
     *
     * @see Indexer#getMetricsTableName
     * @return Metrics table name
     */
    @JsonIgnore
    public String getMetricsTableName()
    {
        return Indexer.getMetricsTableName(schema, table);
    }

    /**
     * Gets the full table name of the Accumulo table, i.e. schemaName.tableName. If the schemaName
     * is 'default', then there is no Accumulo namespace and the table name is all that is returned.
     * This method is ignored by jackson.
     *
     * @see AccumuloClient#getFullTableName
     * @return Full table name
     */
    @JsonIgnore
    public String getFullTableName()
    {
        return AccumuloClient.getFullTableName(schema, table);
    }

    /**
     * Gets all configured columns of the Accumulo table. This method is JsonProperty.
     *
     * @return The list of {@link AccumuloColumnHandle}
     */
    @JsonProperty
    public List<AccumuloColumnHandle> getColumns()
    {
        return columns;
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
     * Gets the list of ColumnMetadata from each AccumuloColumnHandle. This method is ignored by
     * jackson.
     *
     * @return The list of {@link ColumnMetadata}
     */
    @JsonIgnore
    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
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
     * Gets a Boolean value indicating if the Accumulo tables are internal, i.e. managed by Presto.
     * This method is ignored by jackson.
     *
     * @return True if internal, false otherwise
     */
    @JsonIgnore
    public boolean isIndexed()
    {
        return indexed;
    }

    /**
     * Gets the Class object for the configured serializer. This property is ignored by jackson.
     *
     * @return The serializer Class
     */
    @SuppressWarnings("unchecked")
    @JsonIgnore
    public Class<? extends AccumuloRowSerializer> getSerializerClass()
    {
        try {
            return (Class<? extends AccumuloRowSerializer>) Class.forName(serializerClassName);
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(StandardErrorCode.USER_ERROR,
                    "Configured serializer class not found", e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("connectorId", connectorId).add("schemaName", schema)
                .add("tableName", table).add("columns", columns).add("rowIdName", rowId)
                .add("internal", internal).add("serializerClassName", serializerClassName)
                .toString();
    }
}
