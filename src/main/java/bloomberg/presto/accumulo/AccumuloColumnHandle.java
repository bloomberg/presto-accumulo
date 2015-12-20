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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import bloomberg.presto.accumulo.metadata.AccumuloTableMetadataManager;

public final class AccumuloColumnHandle
        implements ColumnHandle, Comparable<AccumuloColumnHandle> {
    private final String connectorId;
    private final String name;
    private final String columnFamily;
    private final String columnQualifier;
    private final Type type;
    private final int ordinal;
    private final String comment;

    @JsonCreator
    public AccumuloColumnHandle(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("columnFamily") String columnFamily,
            @JsonProperty("columnQualifier") String columnQualifier,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinal") int ordinal,
            @JsonProperty("comment") String comment) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "name is null");
        this.columnFamily = name
                .equals(AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME)
                        ? null : requireNonNull(columnFamily, "family is null");
        this.columnQualifier = name.equals(
                AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME) ? null
                        : requireNonNull(columnQualifier, "qualifier is null");
        this.type = requireNonNull(type, "type is null");
        this.ordinal = requireNonNull(ordinal, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getColumnFamily() {
        return columnFamily;
    }

    @JsonProperty
    public String getColumnQualifier() {
        return columnQualifier;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public int getOrdinal() {
        return ordinal;
    }

    @JsonProperty
    public String getComment() {
        return comment;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(name, type, false, comment, false);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, name, columnFamily, columnQualifier,
                type, ordinal, comment);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        AccumuloColumnHandle other = (AccumuloColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.name, other.name)
                && Objects.equals(this.columnFamily, other.columnFamily)
                && Objects.equals(this.columnQualifier, other.columnQualifier)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.ordinal, other.ordinal)
                && Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("connectorId", connectorId)
                .add("name", name).add("columnFamily", columnFamily)
                .add("columnQualifier", columnQualifier).add("type", type)
                .add("ordinal", ordinal).add("comment", comment).toString();
    }

    @Override
    public int compareTo(AccumuloColumnHandle o) {
        return Integer.compare(this.getOrdinal(), o.getOrdinal());
    }
}
