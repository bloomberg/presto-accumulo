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
package bloomberg.presto.accumulo.conf;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.configuration.Config;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

import static java.lang.String.format;

/**
 * File-based configuration properties for the Accumulo connector
 */
public class AccumuloConfig
{
    private String instance;
    private String zooKeepers;
    private String username;
    private String password;
    private String zkMetadataRoot;
    private String metaManClass;
    private Integer cardinalityCacheSize;
    private Integer cardinalityCacheExpireSeconds;

    /**
     * Gets the Accumulo instance name
     *
     * @return Accumulo instance name
     */
    @NotNull
    public String getInstance()
    {
        return this.instance;
    }

    /**
     * Sets the Accumulo instance name
     *
     * @param instance
     *            Accumulo instance name
     * @return this, for chaining
     */
    @Config("instance")
    public AccumuloConfig setInstance(String instance)
    {
        this.instance = instance;
        return this;
    }

    /**
     * Gets the ZooKeeper quorum connect string
     *
     * @return ZooKeeper connect string
     */
    @NotNull
    public String getZooKeepers()
    {
        return this.zooKeepers;
    }

    /**
     * Sets the ZooKeeper quorum connect string
     *
     * @param zooKeepers
     *            ZooKeeper connect string
     * @return this, for chaining
     */
    @Config("zookeepers")
    public AccumuloConfig setZooKeepers(String zooKeepers)
    {
        this.zooKeepers = zooKeepers;
        return this;
    }

    /**
     * Gets the Accumulo user name
     *
     * @return Accumulo user name
     */
    @NotNull
    public String getUsername()
    {
        return this.username;
    }

    /**
     * Sets the user to use when interacting with Accumulo. This user will require administrative
     * permissions
     *
     * @param username
     *            Accumulo user name
     * @return this, for chaining
     */
    @Config("username")
    public AccumuloConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    /**
     * Gets the password for the Accumulo user
     *
     * @return Accumulo password
     */
    @NotNull
    public String getPassword()
    {
        return this.password;
    }

    /**
     * Sets the password for the configured user
     *
     * @param password
     *            Accumulo password
     * @return this, for chaining
     */
    @Config("password")
    public AccumuloConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    /**
     * Gets the root znode for storing the Accumulo metadata, default /presto-accumulo
     *
     * @return Configured metadata root, or /presto-accumulo if not set
     */
    @NotNull
    public String getZkMetadataRoot()
    {
        return zkMetadataRoot == null ? "/presto-accumulo" : zkMetadataRoot;
    }

    /**
     * Sets the root znode for metadata storage
     *
     * @param zkMetadataRoot
     *            Root znode
     */
    @Config("zookeeper.metadata.root")
    public void setZkMetadataRoot(String zkMetadataRoot)
    {
        this.zkMetadataRoot = zkMetadataRoot;
    }

    /**
     * Gets the configured metadata manager. Default is the return value of
     * {@link AccumuloMetadataManager#getDefault}
     *
     * @return Configured AccumuloMetadataManager
     * @throws PrestoException
     *             If an instance of the configured manager is unable to be created
     */
    public AccumuloMetadataManager getMetadataManager()
    {
        try {
            return metaManClass == null || metaManClass.equals("default")
                    ? AccumuloMetadataManager.getDefault(this)
                    : (AccumuloMetadataManager) Class.forName(metaManClass)
                            .getConstructor(AccumuloConfig.class).newInstance(this);
        }
        catch (InstantiationException | IllegalAccessException | ClassNotFoundException
                | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                | SecurityException e) {
            throw new PrestoException(StandardErrorCode.USER_ERROR, e);
        }
    }

    /**
     * Gets the class name of the configured metadata manager. Default is the class name of the
     * class from
     * {@link AccumuloMetadataManager#getDefault}
     *
     * @return Configured AccumuloMetadataManager class name
     */
    @NotNull
    public String getMetadataManagerClass()
    {
        return metaManClass == null || metaManClass.equals("default")
                ? AccumuloMetadataManager.getDefault(this).getClass().getCanonicalName()
                : metaManClass;
    }

    /**
     * Sets the AccumulMetadataManager class
     *
     * @param mmClass
     *            Class name of metadata manager, or default
     */
    @Config("metadata.manager.class")
    public void setMetadataManagerClass(String mmClass)
    {
        this.metaManClass = mmClass;
    }

    /**
     * Gets the size of the index cardinality cache. Default 100000.
     *
     * @return Configured cardinality cache, or 100000 if not set
     */
    @NotNull
    public int getCardinalityCacheSize()
    {
        return cardinalityCacheSize == null ? 100000 : cardinalityCacheSize;
    }

    /**
     * Sets the cardinality cache size
     *
     * @param cardinalityCacheSize
     *            Size of the cache
     */
    @Config("cardinality.cache.size")
    public void setCardinalityCacheSize(int cardinalityCacheSize)
    {
        this.cardinalityCacheSize = cardinalityCacheSize;
    }

    /**
     * Gets the expiration, in seconds, of the cardinality cache. Default 300 aka five minutes.
     *
     * @return Configured cardinality cache expiration, or 300 if not set
     */
    @NotNull
    public int getCardinalityCacheExpireSeconds()
    {
        // 5 minute default
        return cardinalityCacheExpireSeconds == null ? 300 : cardinalityCacheExpireSeconds;
    }

    /**
     * Sets the cardinality cache expiration
     *
     * @param cardinalityCacheExpireSeconds
     *            Cache expiration value
     */
    @Config("cardinality.cache.expire.seconds")
    public void setCardinalityCacheExpireSeconds(int cardinalityCacheExpireSeconds)
    {
        this.cardinalityCacheExpireSeconds = cardinalityCacheExpireSeconds;
    }

    public static AccumuloConfig fromFile(File f)
            throws ConfigurationException
    {
        if (!f.exists() || f.isDirectory()) {
            throw new ConfigurationException(format("File %s does not exist or is a directory", f));
        }
        PropertiesConfiguration props = new PropertiesConfiguration(f);
        props.setThrowExceptionOnMissing(true);

        AccumuloConfig config = new AccumuloConfig();
        config.setCardinalityCacheExpireSeconds(
                props.getInt("cardinality.cache.expire.seconds", 300));
        config.setCardinalityCacheSize(props.getInt("cardinality.cache.size", 100000));
        config.setInstance(props.getString("instance"));
        config.setMetadataManagerClass(props.getString("metadata.manager.class", "default"));
        config.setPassword(props.getString("password"));
        config.setUsername(props.getString("username"));
        config.setZkMetadataRoot(props.getString("zookeeper.metadata.root", "/presto-accumulo"));
        config.setZooKeepers(props.getString("zookeepers"));
        return config;
    }

    public static AccumuloConfig fromURL(URL url)
            throws ConfigurationException
    {
        PropertiesConfiguration props = new PropertiesConfiguration(url);
        props.setThrowExceptionOnMissing(true);

        AccumuloConfig config = new AccumuloConfig();
        config.setCardinalityCacheExpireSeconds(
                props.getInt("cardinality.cache.expire.seconds", 300));
        config.setCardinalityCacheSize(props.getInt("cardinality.cache.size", 100000));
        config.setInstance(props.getString("instance"));
        config.setMetadataManagerClass(props.getString("metadata.manager.class", "default"));
        config.setPassword(props.getString("password"));
        config.setUsername(props.getString("username"));
        config.setZkMetadataRoot(props.getString("zookeeper.metadata.root", "/presto-accumulo"));
        config.setZooKeepers(props.getString("zookeepers"));
        return config;
    }
}
