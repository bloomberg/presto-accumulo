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
package com.facebook.presto.accumulo.tools;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.spi.PrestoException;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.facebook.presto.accumulo.AccumuloErrorCode.MINI_ACCUMULO;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;

public class TestUtils
{
    private static final Logger LOG = Logger.get(TestUtils.class);
    private static AccumuloConfig config = null;
    private static Connector connector = getAccumuloConnector();

    private TestUtils() {}

    public static synchronized AccumuloConfig getAccumuloConfig()
    {
        return config;
    }

    /**
     * Gets the AccumuloConnector singleton, starting the MiniAccumuloCluster on initialization.
     * This singleton instance is required so all test cases access the same MiniAccumuloCluster.
     *
     * @return Accumulo connector
     */
    public static synchronized Connector getAccumuloConnector()
    {
        if (connector != null) {
            return connector;
        }

        try {
            MiniAccumuloCluster accumulo = createMiniAccumuloCluster();

            config = new AccumuloConfig();
            config.setInstance(accumulo.getInstanceName());
            config.setZooKeepers(accumulo.getZooKeepers());
            config.setUsername("root");
            config.setPassword("secret");

            Instance instance = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
            connector = instance.getConnector(config.getUsername(), new PasswordToken(config.getPassword()));
            LOG.info("Connection to MAC instance %s at %s established, user %s password %s", config.getInstance(), config.getZooKeepers(), config.getUsername(), config.getPassword());
            return connector;
        }
        catch (AccumuloException | AccumuloSecurityException | InterruptedException | IOException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to get connector to Accumulo", e);
        }
    }

    /**
     * Creates and starts an instance of MiniAccumuloCluster, returning the new instance.
     *
     * @return New MiniAccumuloCluster
     */
    private static MiniAccumuloCluster createMiniAccumuloCluster()
            throws IOException, InterruptedException
    {
        // Create MAC directory
        File macDir = Files.createTempDirectory("mac-").toFile();
        LOG.info("MAC is enabled, starting MiniAccumuloCluster at %s", macDir);

        // Start MAC and connect to it
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(macDir, "secret");
        accumulo.start();

        // Add shutdown hook to stop MAC and cleanup temporary files
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOG.info("Shutting down MAC");
                accumulo.stop();
            }
            catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PrestoException(MINI_ACCUMULO, "Failed to shut down MAC instance", e);
            }

            try {
                LOG.info("Cleaning up MAC directory");
                FileUtils.forceDelete(macDir);
            }
            catch (IOException e) {
                throw new PrestoException(MINI_ACCUMULO, "Failed to clean up MAC directory", e);
            }
        }));

        return accumulo;
    }
}
