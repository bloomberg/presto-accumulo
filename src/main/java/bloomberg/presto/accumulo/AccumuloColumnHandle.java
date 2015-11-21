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

public final class AccumuloColumnHandle implements ColumnHandle {
    private final String connectorId;
    private final String columnName;
    private final String columnFamily;
    private final String columnQualifier;
    private final Type columnType;
    private final int ordinalPosition;

    @JsonCreator
    public AccumuloColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("ordinalPosition") int ordinalPosition) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.ordinalPosition = ordinalPosition;

        int idx = columnName.indexOf("__");
        this.columnFamily = columnName.substring(0, idx);
        this.columnQualifier = columnName.substring(idx + 2);
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getColumnQualifier() {
        return columnQualifier;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    @JsonProperty
    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, columnType, false);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, columnName);
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
                && Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("connectorId", connectorId)
                .add("columnName", columnName).add("columnType", columnType)
                .add("ordinalPosition", ordinalPosition).toString();
    }
}
