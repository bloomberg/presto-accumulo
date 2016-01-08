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
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import bloomberg.presto.accumulo.model.AccumuloColumnConstraint;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;

public class AccumuloSplit implements ConnectorSplit {
    private final boolean remotelyAccessible;
    private final String connectorId;
    private final String rowIdName;
    private final String schemaName;
    private final String tableName;
    private String serializerClassName;
    private final List<HostAddress> addresses;
    private RangeHandle rHandle;
    private List<AccumuloColumnConstraint> constraints;

    @JsonCreator
    public AccumuloSplit(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("rowIdName") String rowIdName,
            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("rHandle") RangeHandle rHandle,
            @JsonProperty("constraints") List<AccumuloColumnConstraint> constraints) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.rowIdName = requireNonNull(rowIdName, "rowIdName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.serializerClassName = serializerClassName;
        this.constraints = requireNonNull(constraints, "constraints is null");

        // do not "requireNotNull" this field, jackson parses and sets
        // AccumuloSplit, then parses the nested RangeHandle object and will
        // call setRangeHandle, flagged as a JsonSetter
        this.rHandle = rHandle;

        remotelyAccessible = true;
        addresses = newArrayList(HostAddress.fromString("127.0.0.1"));
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getRowIdName() {
        return rowIdName;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonGetter
    public String getSerializerClassName() {
        return this.serializerClassName;
    }

    @JsonProperty
    public RangeHandle getRangeHandle() {
        return rHandle;
    }

    @JsonSetter
    public void setRangeHandle(RangeHandle rhandle) {
        this.rHandle = rhandle;
    }

    @JsonProperty
    public List<AccumuloColumnConstraint> getConstraints() {
        return constraints;
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

    @Override
    public boolean isRemotelyAccessible() {
        return remotelyAccessible;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("connectorId", connectorId)
                .add("schemaName", schemaName).add("tableName", tableName)
                .add("rowIdName", rowIdName)
                .add("serializerClassName", serializerClassName)
                .add("remotelyAccessible", remotelyAccessible)
                .add("addresses", addresses).add("rHandle", rHandle)
                .add("constraints", constraints).toString();
    }
}
