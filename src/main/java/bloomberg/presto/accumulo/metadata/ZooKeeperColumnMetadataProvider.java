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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.activity.InvalidActivityException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.AccumuloColumnHandle;
import bloomberg.presto.accumulo.AccumuloConfig;
import bloomberg.presto.accumulo.AccumuloTable;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;

public class ZooKeeperColumnMetadataProvider
        extends AccumuloTableMetadataManager {

    private static final String DEFAULT_SCHEMA = "default";
    private static final Logger LOG = Logger
            .get(ZooKeeperColumnMetadataProvider.class);

    private final CuratorFramework curator;
    private final ObjectMapper mapper;
    private final String zkMetadataRoot;
    private final String zookeepers;

    public ZooKeeperColumnMetadataProvider(String connectorId,
            AccumuloConfig config) {
        super(connectorId, config);
        zkMetadataRoot = config.getZkMetadataRoot();
        zookeepers = config.getZooKeepers();

        CuratorFramework checkRoot = CuratorFrameworkFactory
                .newClient(zookeepers, new ExponentialBackoffRetry(1000, 3));
        checkRoot.start();

        try {
            if (checkRoot.checkExists().forPath(zkMetadataRoot) == null) {
                checkRoot.create().forPath(zkMetadataRoot);
            }
        } catch (Exception e) {
            throw new RuntimeException("ZK error checking metadata root", e);
        }
        checkRoot.close();

        curator = CuratorFrameworkFactory.newClient(zookeepers + zkMetadataRoot,
                new ExponentialBackoffRetry(1000, 3));
        curator.start();

        try {
            if (curator.checkExists().forPath("/" + DEFAULT_SCHEMA) == null) {
                curator.create().forPath("/" + DEFAULT_SCHEMA);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "ZK error checking/creating default schema", e);
        }

        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(
                ImmutableMap.<Class<?>, JsonDeserializer<?>> of(Type.class,
                        new TypeDeserializer(new TypeRegistry())));
        mapper = objectMapperProvider.get();
    }

    @Override
    public Set<String> getSchemaNames() {
        try {
            Set<String> schemas = new HashSet<>();
            schemas.addAll(curator.getChildren().forPath("/"));
            return schemas;
        } catch (Exception e) {
            throw new RuntimeException("Error fetching schemas", e);
        }
    }

    @Override
    public Set<String> getTableNames(String schema) {
        boolean exists;
        try {
            exists = curator.checkExists().forPath("/" + schema) != null;
        } catch (Exception e) {
            throw new RuntimeException("Error checking if schema exists", e);
        }

        if (exists) {
            try {
                Set<String> tables = new HashSet<>();
                tables.addAll(curator.getChildren().forPath("/" + schema));
                return tables;
            } catch (Exception e) {
                throw new RuntimeException("Error fetching schemas", e);
            }
        } else {
            throw new RuntimeException("No metadata for schema " + schema);
        }
    }

    @Override
    public AccumuloTable getTable(SchemaTableName stName) {
        try {
            if (curator.checkExists().forPath(getTablePath(stName)) != null) {
                return new AccumuloTable(
                        AccumuloClient.getFullTableName(stName),
                        getColumnHandles(stName));
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error fetching table", e);
        }
    }

    @Override
    public List<AccumuloColumnHandle> getColumnHandles(SchemaTableName stName) {
        try {
            String schemaPath = getSchemaPath(stName);
            String tablePath = getTablePath(stName);
            List<AccumuloColumnHandle> columns = new ArrayList<>();

            columns.add(super.getRowIdColumn());

            if (curator.checkExists().forPath(schemaPath) != null) {
                if (curator.checkExists().forPath(tablePath) != null) {
                    for (String colName : curator.getChildren()
                            .forPath(tablePath)) {
                        String colPath = tablePath + "/" + colName;
                        AccumuloColumnHandle col = toAccumuloColumn(
                                curator.getData().forPath(colPath));
                        columns.add(col);
                        LOG.debug(col.toString());
                    }
                } else {
                    throw new InvalidActivityException(
                            String.format("No known metadata for %s ", stName));
                }
            } else {
                throw new InvalidActivityException(
                        String.format("No known metadata for schema %s ",
                                stName.getSchemaName()));
            }

            Collections.sort(columns);

            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Error fetching metadata", e);
        }
    }

    @Override
    public AccumuloColumnHandle getColumnHandle(SchemaTableName stName,
            String name) {
        try {
            if (name.equals(AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME)) {
                return super.getRowIdColumn();
            } else {
                return toAccumuloColumn(curator.getData().forPath(
                        String.format("%s/%s", getTablePath(stName), name)));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error fetching metadata", e);
        }
    }

    @Override
    public void createTableMetadata(SchemaTableName stName,
            List<AccumuloColumnHandle> columns) {
        String tablePath = getTablePath(stName);

        boolean exists;
        try {
            exists = curator.checkExists().forPath(tablePath) != null;
        } catch (Exception e) {
            throw new RuntimeException(
                    "ZK error when checking if table already exists", e);
        }

        try {
            if (!exists) {
                ZooKeeperMetadataCreator creator = new ZooKeeperMetadataCreator();
                creator.setZooKeepers(zookeepers);
                creator.setNamespace(stName.getSchemaName());
                creator.setTable(stName.getTableName());
                creator.setMetadataRoot(zkMetadataRoot);

                for (AccumuloColumnHandle c : columns) {
                    if (!c.getName().equals(
                            AccumuloTableMetadataManager.ROW_ID_COLUMN_NAME)) {
                        LOG.debug("Creating " + c);
                        creator.setColumnFamily(c.getColumnFamily());
                        creator.setColumnQualifier(c.getColumnQualifier());
                        creator.setPrestoColumn(c.getName());
                        creator.setPrestoType(c.getType().toString());
                        creator.createMetadata();
                    }
                }
            } else {
                throw new InvalidActivityException(String.format(
                        "Metadata for table %s already exists", stName));
            }
        } catch (Exception e) {
            throw new RuntimeException("ZK error when creating metatadata", e);
        }
    }

    @Override
    public void deleteTableMetadata(SchemaTableName stName) {
        try {
            curator.delete().deletingChildrenIfNeeded()
                    .forPath(getTablePath(stName));
        } catch (Exception e) {
            throw new RuntimeException("ZK error when deleting metatadata", e);
        }
    }

    private String getSchemaPath(SchemaTableName stName) {
        return "/" + stName.getSchemaName();
    }

    private String getTablePath(SchemaTableName stName) {
        return getSchemaPath(stName) + '/' + stName.getTableName();
    }

    private AccumuloColumnHandle toAccumuloColumn(byte[] data)
            throws JsonParseException, JsonMappingException, IOException {
        return mapper.readValue(new String(data), AccumuloColumnHandle.class);
    }
}
