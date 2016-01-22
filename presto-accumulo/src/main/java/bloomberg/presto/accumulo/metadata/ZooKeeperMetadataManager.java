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
package bloomberg.presto.accumulo.metadata;

import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloTable;
import com.facebook.presto.spi.SchemaTableName;
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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import javax.activity.InvalidActivityException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ZooKeeperMetadataManager
        extends AccumuloMetadataManager
{
    private static final String DEFAULT_SCHEMA = "default";
    private static final Logger LOG = Logger.get(ZooKeeperMetadataManager.class);

    private final CuratorFramework curator;
    private final ObjectMapper mapper;
    private final String zkMetadataRoot;
    private final String zookeepers;

    public ZooKeeperMetadataManager(String connectorId, AccumuloConfig config)
    {
        super(connectorId, config);
        zkMetadataRoot = config.getZkMetadataRoot();
        zookeepers = config.getZooKeepers();

        CuratorFramework checkRoot = CuratorFrameworkFactory.newClient(zookeepers, new ExponentialBackoffRetry(1000, 3));
        checkRoot.start();

        try {
            if (checkRoot.checkExists().forPath(zkMetadataRoot) == null) {
                checkRoot.create().forPath(zkMetadataRoot);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("ZK error checking metadata root", e);
        }
        checkRoot.close();

        curator = CuratorFrameworkFactory.newClient(zookeepers + zkMetadataRoot, new ExponentialBackoffRetry(1000, 3));
        curator.start();

        try {
            if (curator.checkExists().forPath("/" + DEFAULT_SCHEMA) == null) {
                curator.create().forPath("/" + DEFAULT_SCHEMA);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("ZK error checking/creating default schema", e);
        }

        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TypeDeserializer(new TypeRegistry())));
        mapper = objectMapperProvider.get();
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try {
            Set<String> schemas = new HashSet<>();
            schemas.addAll(curator.getChildren().forPath("/"));
            return schemas;
        }
        catch (Exception e) {
            throw new RuntimeException("Error fetching schemas", e);
        }
    }

    @Override
    public Set<String> getTableNames(String schema)
    {
        boolean exists;
        try {
            exists = curator.checkExists().forPath("/" + schema) != null;
        }
        catch (Exception e) {
            throw new RuntimeException("Error checking if schema exists", e);
        }

        if (exists) {
            try {
                Set<String> tables = new HashSet<>();
                tables.addAll(curator.getChildren().forPath("/" + schema));
                return tables;
            }
            catch (Exception e) {
                throw new RuntimeException("Error fetching schemas", e);
            }
        }
        else {
            throw new RuntimeException("No metadata for schema " + schema);
        }
    }

    @Override
    public AccumuloTable getTable(SchemaTableName stName)
    {
        try {
            if (curator.checkExists().forPath(getTablePath(stName)) != null) {
                return toAccumuloTable(curator.getData().forPath(getTablePath(stName)));
            }
            else {
                LOG.info("No metadata for table " + stName);
                return null;
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error fetching table", e);
        }
    }

    @Override
    public void createTableMetadata(AccumuloTable table)
    {
        String tablePath = getTablePath(table.toSchemaTableName());

        try {
            if (curator.checkExists().forPath(tablePath) != null) {
                throw new InvalidActivityException(String.format("Metadata for table %s already exists", table.toSchemaTableName()));

            }
        }
        catch (Exception e) {
            throw new RuntimeException("ZK error when checking if table already exists", e);
        }

        try {
            curator.create().creatingParentsIfNeeded().forPath(tablePath, toJsonBytes(table));
        }
        catch (Exception e) {
            throw new RuntimeException("Error creating table node in ZooKeeper", e);
        }
    }

    @Override
    public void deleteTableMetadata(SchemaTableName stName)
    {
        try {
            curator.delete().deletingChildrenIfNeeded().forPath(getTablePath(stName));
        }
        catch (Exception e) {
            throw new RuntimeException("ZK error when deleting metatadata", e);
        }
    }

    private String getSchemaPath(SchemaTableName stName)
    {
        return "/" + stName.getSchemaName();
    }

    private String getTablePath(SchemaTableName stName)
    {
        return getSchemaPath(stName) + '/' + stName.getTableName();
    }

    private AccumuloTable toAccumuloTable(byte[] data)
            throws JsonParseException, JsonMappingException, IOException
    {
        return mapper.readValue(new String(data), AccumuloTable.class);
    }

    private byte[] toJsonBytes(AccumuloTable t)
            throws JsonParseException, JsonMappingException, IOException
    {
        return mapper.writeValueAsBytes(t);
    }
}
