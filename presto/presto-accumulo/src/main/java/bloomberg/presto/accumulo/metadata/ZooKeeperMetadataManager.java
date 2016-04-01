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

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.log.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import javax.activity.InvalidActivityException;

import java.util.HashSet;
import java.util.Set;

/**
 * An implementation of {@link AccumuloMetadataManager} that persists metadata to Apache ZooKeeper.
 */
public class ZooKeeperMetadataManager
        extends AccumuloMetadataManager
{
    private static final String DEFAULT_SCHEMA = "default";
    private static final Logger LOG = Logger.get(ZooKeeperMetadataManager.class);

    private final CuratorFramework curator;
    private final String zkMetadataRoot;
    private final String zookeepers;

    /**
     * Creates a new instance of {@link ZooKeeperMetadataManager}
     *
     * @param config Connector configuration for Accumulo
     */
    public ZooKeeperMetadataManager(AccumuloConfig config)
    {
        super(config);
        zkMetadataRoot = config.getZkMetadataRoot();
        zookeepers = config.getZooKeepers();

        // Create the connection to ZooKeeper to check if the metadata root exists
        CuratorFramework checkRoot =
                CuratorFrameworkFactory.newClient(zookeepers, new ExponentialBackoffRetry(1000, 3));
        checkRoot.start();

        try {
            // If the metadata root does not exist, create it
            if (checkRoot.checkExists().forPath(zkMetadataRoot) == null) {
                checkRoot.create().forPath(zkMetadataRoot);
            }
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "ZK error checking metadata root", e);
        }
        checkRoot.close();

        // Create the curator client framework to use for metadata management, set at the ZK root
        curator = CuratorFrameworkFactory.newClient(zookeepers + zkMetadataRoot,
                new ExponentialBackoffRetry(1000, 3));
        curator.start();

        try {
            // Create default schema should it not exist
            if (curator.checkExists().forPath("/" + DEFAULT_SCHEMA) == null) {
                curator.create().forPath("/" + DEFAULT_SCHEMA);
            }
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "ZK error checking/creating default schema", e);
        }
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
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Error fetching schemas",
                    e);
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
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Error checking if schema exists", e);
        }

        if (exists) {
            try {
                Set<String> tables = new HashSet<>();
                tables.addAll(curator.getChildren().forPath("/" + schema));
                return tables;
            }
            catch (Exception e) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                        "Error fetching schemas", e);
            }
        }
        else {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "No metadata for schema" + schema);
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
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "Error fetching table", e);
        }
    }

    @Override
    public void createTableMetadata(AccumuloTable table)
    {
        SchemaTableName stn = new SchemaTableName(table.getSchema(), table.getTable());
        String tablePath = getTablePath(stn);

        try {
            if (curator.checkExists().forPath(tablePath) != null) {
                throw new InvalidActivityException(
                        String.format("Metadata for table %s already exists", stn));
            }
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "ZK error when checking if table already exists: " + e.getMessage(), e);
        }

        try {
            curator.create().creatingParentsIfNeeded().forPath(tablePath, toJsonBytes(table));
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "Error creating table node in ZooKeeper", e);
        }
    }

    @Override
    public void deleteTableMetadata(SchemaTableName stName)
    {
        try {
            curator.delete().deletingChildrenIfNeeded().forPath(getTablePath(stName));
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "ZK error when deleting metadata", e);
        }
    }

    /**
     * Gets the schema znode for the given schema table name
     *
     * @param stn Schema table name
     * @return The path for the schema node
     */
    private String getSchemaPath(SchemaTableName stn)
    {
        return "/" + stn.getSchemaName();
    }

    /**
     * Gets the table znode for the given table name
     *
     * @param stn Schema table name
     * @return The path for the table
     */
    private String getTablePath(SchemaTableName stn)
    {
        return getSchemaPath(stn) + '/' + stn.getTableName();
    }
}