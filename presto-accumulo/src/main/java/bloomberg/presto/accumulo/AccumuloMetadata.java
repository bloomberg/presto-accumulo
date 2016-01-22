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

import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static bloomberg.presto.accumulo.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AccumuloMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final AccumuloClient client;

    @Inject
    public AccumuloMetadata(AccumuloConnectorId connectorId, AccumuloClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName stName = tableMetadata.getTable();
        AccumuloTable table = client.createTable(tableMetadata);
        return new AccumuloTableHandle(connectorId, stName.getSchemaName(), stName.getTableName(), table.getRowIdName(), table.isInternal(), table.getSerializerClass().getName());
    }

    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        // no-op?
    }

    public void rollbackCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        AccumuloTableHandle th = checkType(tableHandle, AccumuloTableHandle.class, "table");
        client.dropTable(th.toSchemaTableName(), th.isInternal());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        client.createTable(tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle th = checkType(tableHandle, AccumuloTableHandle.class, "table");
        client.dropTable(th.toSchemaTableName(), th.isInternal());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // Unsure what to do here besides validate the type
        // Seems like this is mostly metadata management
        // The bulk of the work is done in the page sink for building out
        // the pages (blocks of rows of data)
        return checkType(tableHandle, AccumuloTableHandle.class, "table");
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        // Seems like most connectors just return metadata about the fragments
        // Unsure if we can use this for an optimization?
        // Priority is on reading though, so we can investigate this later
        checkType(insertHandle, AccumuloTableHandle.class, "table");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(client.getSchemaNames());
    }

    @Override
    public AccumuloTableHandle getTableHandle(ConnectorSession session, SchemaTableName stName)
    {
        if (!listSchemaNames(session).contains(stName.getSchemaName())) {
            return null;
        }

        AccumuloTable table = client.getTable(stName);
        if (table == null) {
            return null;
        }

        return new AccumuloTableHandle(connectorId, stName.getSchemaName(), stName.getTableName(), table.getRowIdName(), table.isInternal(), table.getSerializerClass().getName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        AccumuloTableHandle tableHandle = checkType(table, AccumuloTableHandle.class, "table");

        ConnectorTableLayout layout = new ConnectorTableLayout(new AccumuloTableLayoutHandle(tableHandle, constraint.getSummary()));

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        AccumuloTableLayoutHandle layout = checkType(handle, AccumuloTableLayoutHandle.class, "layout");
        return new ConnectorTableLayout(layout);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        AccumuloTableHandle tHandle = checkType(table, AccumuloTableHandle.class, "table");
        checkArgument(tHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(tHandle.getSchemaName(), tHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = client.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : client.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AccumuloTableHandle tHandle = checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        checkArgument(tHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        AccumuloTable table = client.getTable(tHandle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(tHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (AccumuloColumnHandle column : table.getColumns()) {
            columnHandles.put(column.getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, AccumuloTableHandle.class, "tableHandle");
        return checkType(columnHandle, AccumuloColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName stName)
    {
        if (!listSchemaNames().contains(stName.getSchemaName())) {
            return null;
        }

        AccumuloTable table = client.getTable(stName);
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(stName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }
}
