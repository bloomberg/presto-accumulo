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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

import java.util.List;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;

public class AccumuloTable {
    private final String schemaName;
    private final String tableName;
    private final List<AccumuloColumnHandle> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final String serializerClassName;

    @JsonCreator
    public AccumuloTable(@JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<AccumuloColumnHandle> columns,
            @JsonProperty("serializerClassName") String serializerClassName) {
        checkArgument(!isNullOrEmpty(schemaName),
                "schemaName is null or is empty");
        checkArgument(!isNullOrEmpty(tableName),
                "tableName is null or is empty");
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = ImmutableList
                .copyOf(requireNonNull(columns, "columns are null"));
        this.serializerClassName = requireNonNull(serializerClassName,
                "serializerClassName is null");

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList
                .builder();
        for (AccumuloColumnHandle column : this.columns) {
            columnsMetadata.add(column.getColumnMetadata());
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonIgnore
    public String getFullTableName() {
        return schemaName.equals("default") ? tableName
                : schemaName + "." + tableName;
    }

    @JsonProperty
    public List<AccumuloColumnHandle> getColumns() {
        return columns;
    }

    @JsonGetter
    public String getSerializerClassName() {
        return serializerClassName;
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }

    @SuppressWarnings("unchecked")
    @JsonIgnore
    public Class<? extends AccumuloRowSerializer> getSerializerClass() {
        try {
            return (Class<? extends AccumuloRowSerializer>) Class
                    .forName(serializerClassName);
        } catch (ClassNotFoundException e) {
            throw new PrestoException(StandardErrorCode.USER_ERROR,
                    "Configured serializer class not found", e);
        }
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("schemaName", schemaName)
                .add("tableName", tableName).add("columns", columns)
                .add("serializerClassName", serializerClassName).toString();
    }
}
