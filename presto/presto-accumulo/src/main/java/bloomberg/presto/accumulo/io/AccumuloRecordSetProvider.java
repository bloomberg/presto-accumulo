/*
 * Copyright 2016 Bloomberg L.P.
 *
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
package bloomberg.presto.accumulo.io;

import bloomberg.presto.accumulo.AccumuloConnectorId;
import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.AccumuloSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import javax.inject.Inject;

import java.util.List;

import static bloomberg.presto.accumulo.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of a ConnectorRecordSetProvider for Accumulo. Generates {@link AccumuloRecordSet}
 * objects for a provided split.
 *
 * @see AccumuloRecordSet
 * @see AccumuloRecordCursor
 */
public class AccumuloRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final AccumuloConfig config;
    private final Connector conn;

    @Inject
    public AccumuloRecordSetProvider(AccumuloConnectorId connectorId, AccumuloConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.config = requireNonNull(config, "config is null");

        ZooKeeperInstance inst =
                new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        try {
            conn = inst.getConnector(config.getUsername(),
                    new PasswordToken(config.getPassword().getBytes()));
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }

    /**
     * Gets a record set to read data from the given split and columns.
     *
     * @param session
     *            Current client session
     * @param split
     *            Split to read
     * @param columns
     *            Column handles of the table
     */
    @Override
    public RecordSet getRecordSet(ConnectorSession session, ConnectorSplit split,
            List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        // Convert split
        AccumuloSplit accSplit = checkType(split, AccumuloSplit.class, "split");
        checkArgument(accSplit.getConnectorId().equals(connectorId),
                "split is not for this connector");

        // Convert all columns handles
        ImmutableList.Builder<AccumuloColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add(checkType(handle, AccumuloColumnHandle.class, "handle"));
        }

        // Return new record set
        return new AccumuloRecordSet(session, config, accSplit, handles.build(), conn);
    }
}
