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
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;

public class AccumuloSplitManager implements ConnectorSplitManager {
    private final String connectorId;
    private final AccumuloClient client;

    @Inject
    public AccumuloSplitManager(AccumuloConnectorId connectorId,
            AccumuloClient client) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null")
                .toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session,
            ConnectorTableLayoutHandle layout) {
        AccumuloTableLayoutHandle layoutHandle = checkType(layout,
                AccumuloTableLayoutHandle.class, "layout");
        AccumuloTableHandle tableHandle = layoutHandle.getTable();
        AccumuloTable table = client.getTable(tableHandle.getSchemaName(),
                tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists",
                tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new AccumuloSplit(connectorId, tableHandle.getSchemaName(),
                tableHandle.getTableName()));
        Collections.shuffle(splits);

        return new FixedSplitSource(connectorId, splits);
    }
}
