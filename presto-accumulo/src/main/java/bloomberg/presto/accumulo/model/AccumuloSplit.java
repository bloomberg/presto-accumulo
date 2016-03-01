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

import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.accumulo.core.data.Range;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.requireNonNull;

/**
 * Jackson object and an implementation of a Presto ConnectorSplit. Encapsulates all data regarding
 * a split of Accumulo table for Presto to scan data from.
 */
public class AccumuloSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String hostPort;
    private final String rowId;
    private final String schema;
    private final String table;
    private String serializerClassName;
    private final List<HostAddress> addresses;
    private List<RangeHandle> rHandles;
    private List<AccumuloColumnConstraint> constraints;

    /**
     * JSON creator for an {@link AccumuloSplit}
     *
     * @param connectorId
     *            Presto connector ID
     * @param schema
     *            Schema name
     * @param table
     *            Table name
     * @param rowId
     *            Presto column mapping to the Accumulo row ID
     * @param serializerClassName
     *            Serializer class name for deserializing data stored in Accumulo
     * @param rHandles
     *            List of Accumulo RangeHandles (wrapper for Range) for this split
     * @param constraints
     *            List of constraints
     * @param hostPort
     *            TabletServer host:port to give Presto a hint
     */
    @JsonCreator
    public AccumuloSplit(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema, @JsonProperty("table") String table,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("rHandles") List<RangeHandle> rHandles,
            @JsonProperty("constraints") List<AccumuloColumnConstraint> constraints,
            @JsonProperty("hostPort") String hostPort)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.serializerClassName = serializerClassName;
        this.constraints = requireNonNull(constraints, "constraints is null");
        this.hostPort = requireNonNull(hostPort, "hostPort is null");

        // We don't "requireNotNull" this field, Jackson parses objects using a top-down approach,
        // first parsing the AccumuloSplit, then parsing the nested RangeHandle object.
        // Jackson will call setRangeHandle instead
        this.rHandles = rHandles;

        // Parse the host address into a list of addresses, this would be an Accumulo Tablet server,
        // or some localhost thing
        addresses = newArrayList(HostAddress.fromString(hostPort));
    }

    /**
     * Gets the Presto connector ID. This is a JSON property.
     *
     * @return Connector ID
     */
    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    /**
     * Gets the host:port string. This is a JSON property.
     *
     * @return Host and port
     */
    @JsonProperty
    public String getHostPort()
    {
        return hostPort;
    }

    /**
     * Gets the Presto column name that is the Accumulo row ID
     *
     * @return Row ID column
     */
    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    /**
     * Gets the schema name of the Accumulo table
     *
     * @return Schema name
     */
    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    /**
     * Gets the table name
     *
     * @return Table name
     */
    @JsonProperty
    public String getTable()
    {
        return table;
    }

    /**
     * Gets the full Accumulo table name, including namespace and table name
     *
     * @return Full table name
     */
    @JsonIgnore
    public String getFullTableName()
    {
        return (this.getSchema().equals("default") ? "" : this.getSchema() + ".") + this.getTable();
    }

    @JsonGetter
    public String getSerializerClassName()
    {
        return this.serializerClassName;
    }

    @JsonProperty
    public List<RangeHandle> getRangeHandles()
    {
        return rHandles;
    }

    @JsonIgnore
    public List<Range> getRanges()
    {
        List<Range> ranges = new ArrayList<>();
        rHandles.stream().forEach(x -> ranges.add(x.getRange()));
        return ranges;
    }

    @JsonSetter
    public void setRangeHandles(List<RangeHandle> rhandles)
    {
        this.rHandles = rhandles;
    }

    @JsonProperty
    public List<AccumuloColumnConstraint> getConstraints()
    {
        return constraints;
    }

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
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("connectorId", connectorId).add("schema", schema)
                .add("table", table).add("rowId", rowId)
                .add("serializerClassName", serializerClassName).add("addresses", addresses)
                .add("numHandles", rHandles.size()).add("constraints", constraints)
                .add("hostPort", hostPort).toString();
    }
}
