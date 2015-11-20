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

import static bloomberg.presto.accumulo.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.List;

import javax.inject.Inject;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

public class AccumuloRecordSetProvider implements ConnectorRecordSetProvider {
    private final String connectorId;

    @Inject
    public AccumuloRecordSetProvider(AccumuloConnectorId connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null")
                .toString();
    }

    @Override
    public RecordSet getRecordSet(ConnectorSession session,
            ConnectorSplit split, List<? extends ColumnHandle> columns) {
        requireNonNull(split, "partitionChunk is null");
        AccumuloSplit exampleSplit = checkType(split, AccumuloSplit.class,
                "split");
        checkArgument(exampleSplit.getConnectorId().equals(connectorId),
                "split is not for this connector");

        ImmutableList.Builder<AccumuloColumnHandle> handles = ImmutableList
                .builder();
        for (ColumnHandle handle : columns) {
            handles.add(checkType(handle, AccumuloColumnHandle.class, "handle"));
        }

        return new AccumuloRecordSet(exampleSplit, handles.build());
    }
}
