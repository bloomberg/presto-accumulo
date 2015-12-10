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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.activity.InvalidActivityException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;

public class ZooKeeperColumnMetadataProvider
        extends AccumuloColumnMetadataProvider {

    private static final Logger LOG = Logger
            .get(ZooKeeperColumnMetadataProvider.class);
    private final CuratorFramework client;
    private final ObjectMapper mapper;

    public ZooKeeperColumnMetadataProvider(AccumuloConfig config) {
        LOG.debug("constructor");
        requireNonNull(config, "config is null");
        CuratorFramework checkRoot = CuratorFrameworkFactory.newClient(
                config.getZooKeepers(), new ExponentialBackoffRetry(1000, 3));
        checkRoot.start();

        try {
            if (checkRoot.checkExists()
                    .forPath(config.getZkMetadataRoot()) == null) {
                throw new Exception(
                        String.format("ZK metadata root %s does not exist",
                                config.getZkMetadataRoot()));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error connecting to ZooKeeper for fetching metadata", e);
        }
        checkRoot.close();

        client = CuratorFrameworkFactory.newClient(
                config.getZooKeepers() + config.getZkMetadataRoot(),
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.<Class<?>, JsonDeserializer<?>> of(Type.class,
                        new TypeDeserializer(new TypeRegistry())));
        mapper = objectMapperProvider.get();
    }

    @Override
    public List<AccumuloColumn> getColumnMetadata(String schema, String table) {
        try {
            String schemaPath = "/" + schema;
            String tablePath = schemaPath + '/' + table;
            List<AccumuloColumn> columns = new ArrayList<>();

            columns.add(super.getRowIdColumn());

            if (client.checkExists().forPath(schemaPath) != null) {
                if (client.checkExists().forPath(tablePath) != null) {
                    for (String colName : client.getChildren()
                            .forPath(tablePath)) {
                        String colPath = tablePath + "/" + colName;
                        columns.add(toAccumuloColumn(
                                client.getData().forPath(colPath)));
                    }
                } else {
                    throw new InvalidActivityException(String.format(
                            "No known metadata for table %s in schema %s ",
                            table, schema));
                }
            } else {
                throw new InvalidActivityException(
                        "No known metadata for schema " + schema);
            }

            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Error fetching metadata", e);
        }
    }

    @Override
    public AccumuloColumn getAccumuloColumn(String schema, String table,
            String colName) {
        try {
            if (colName.equals(
                    AccumuloColumnMetadataProvider.ROW_ID_COLUMN_NAME)) {
                return super.getRowIdColumn();
            } else {
                return toAccumuloColumn(client.getData().forPath(
                        String.format("/%s/%s/%s", schema, table, colName)));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error fetching metadata", e);
        }
    }

    private AccumuloColumn toAccumuloColumn(byte[] data)
            throws JsonParseException, JsonMappingException, IOException {
        return mapper.readValue(new String(data), AccumuloColumn.class);
    }
}
