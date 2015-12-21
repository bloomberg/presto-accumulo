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
import static java.util.Objects.requireNonNull;

import javax.inject.Inject;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;

import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import io.airlift.log.Logger;

public class AccumuloPageSinkProvider implements ConnectorPageSinkProvider {
    private static final Logger LOG = Logger
            .get(AccumuloPageSinkProvider.class);
    private final AccumuloClient client;
    private final Connector conn;
    private final AccumuloRowSerializer serializer;

    @Inject
    public AccumuloPageSinkProvider(AccumuloConfig config,
            AccumuloClient client) {
        LOG.debug("constructor");
        this.client = requireNonNull(client, "client is null");
        requireNonNull(config, "config is null");
        this.serializer = config.getAccumuloRowSerializer();

        ZooKeeperInstance inst = new ZooKeeperInstance(config.getInstance(),
                config.getZooKeepers());
        try {
            conn = inst.getConnector(config.getUsername(),
                    new PasswordToken(config.getPassword().getBytes()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        AccumuloTableHandle tHandle = checkType(outputTableHandle,
                AccumuloTableHandle.class, "table");
        AccumuloTable table = client.getTable(tHandle.toSchemaTableName());
        return new AccumuloPageSink(conn, table, serializer);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle) {
        AccumuloTableHandle tHandle = checkType(insertTableHandle,
                AccumuloTableHandle.class, "table");
        AccumuloTable table = client.getTable(tHandle.toSchemaTableName());
        return new AccumuloPageSink(conn, table, serializer);
    }
}
