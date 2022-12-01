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
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.io.AccumuloPageSink;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.accumulo.model.RowSchema;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;

/**
 * This task is intended to be used as a wrapper of Accumulo's BatchWriter object. This leverages
 * the Presto metadata, applying the appropriate index Mutations to the index table (if applicable)
 * as well as writing the given Mutations to the main data table. The task itself can be used to
 * ingest a delimited file into Presto, but is intended to be used programatically.
 */
public class PrestoBatchWriter
        extends Task
{
    public static final String TASK_NAME = "batchwriter";
    public static final String DESCRIPTION =
            "Writes rows of data from a file to a Presto table via an Accumulo BatchWriter.  "
                    + "Writes any index mutations if appropriate";

    private static final Logger LOG = Logger.getLogger(PrestoBatchWriter.class);

    // Options
    private static final char SCHEMA_OPT = 's';
    private static final char TABLE_OPT = 't';
    private static final char PATH_OPT = 'f';
    private static final char DELIMITER_OPT = 'd';

    // User-configured values
    private AccumuloConfig config;
    private String schema;
    private String tableName;
    private Path path;
    private BatchWriterConfig bwc;
    private char delimiter = ',';

    // Created by task
    private int numMutations;
    private AccumuloTable table = null;
    private BatchWriter writer = null;
    private Indexer indexer = null;

    /**
     * Initializes the {@link PrestoBatchWriter}, connecting to Accumulo and creating a BatchWriter
     * for the corresponding Presto table, as well leveraging the {@link Indexer} utility to index
     * the data. Call this method after the various set methods are called.
     *
     * @throws AccumuloException If a generic Accumulo error occurs
     * @throws AccumuloSecurityException If a security exception occurs
     * @throws TableNotFoundException If a table is not found in Accumulo
     */
    public void init()
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        // Create the instance and the connector
        Instance inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        Connector connector = inst.getConnector(config.getUsername(), new PasswordToken(config.getPassword()));

        // Fetch the table metadata
        // TODO: construct real TypeManager
        ZooKeeperMetadataManager manager = new ZooKeeperMetadataManager(config, createTestFunctionAndTypeManager());
        this.table = manager.getTable(new SchemaTableName(schema, tableName));

        if (this.table == null) {
            throw new InvalidParameterException(format("No metadata for %s.%s", schema, tableName));
        }

        // Get the scan authorizations based on the class's configuration or the user name
        final Authorizations auths;
        if (table.getScanAuthorizations().isPresent()) {
            auths = new Authorizations(table.getScanAuthorizations().get().split(","));
        }
        else {
            auths = connector.securityOperations().getUserAuthorizations(config.getUsername());
        }

        // If no batch writer config has been set, then create a default config
        if (bwc == null) {
            bwc = new BatchWriterConfig();
        }

        // Create the batch writer and the index tool if this table is indexed.

        writer = connector.createBatchWriter(table.getFullTableName(), bwc);
        if (table.isIndexed()) {
            indexer = new Indexer(connector, auths, table, bwc);
            LOG.info(format("Created writer and indexer for table %s", table.getFullTableName()));
        }
        else {
            LOG.info(format("Created writer for table %s -- table is not indexed", table.getFullTableName()));
        }
    }

    /**
     * Gets the {@link AccumuloTable} object. Only valid after {@link PrestoBatchWriter#init} is
     * called
     *
     * @return AccumuloTable, or null if init was not called
     */
    public AccumuloTable getAccumuloTable()
    {
        return this.table;
    }

    /**
     * Add a mutation to the data and index tables (if appropriate)
     *
     * @param m Mutation to add
     * @throws MutationsRejectedException If the mutation is rejected
     */
    public void addMutation(Mutation m)
            throws MutationsRejectedException
    {
        writer.addMutation(m);
        if (indexer != null) {
            indexer.index(m);
        }
        ++numMutations;
    }

    /**
     * Add the collection of Mutations to the data and index tables (if appropriate)
     *
     * @param m Iterable object of mutations
     * @throws MutationsRejectedException If the mutation is rejected
     */
    public void addMutations(Iterable<Mutation> m)
            throws MutationsRejectedException
    {
        writer.addMutations(m);
        if (indexer != null) {
            indexer.index(m);
        }
    }

    /**
     * Flushes the data to the main table and the {@link Indexer}.
     *
     * @throws MutationsRejectedException If a mutation is rejected
     */
    public void flush()
            throws MutationsRejectedException
    {
        if (indexer != null) {
            indexer.flush();
        }
        writer.flush();
    }

    /**
     * Flushes and closes the BatchWriter and the {@link Indexer}
     *
     * @throws MutationsRejectedException If a mutation is rejected
     */
    public void close()
            throws MutationsRejectedException
    {
        if (indexer != null) {
            indexer.close();
        }
        writer.close();

        LOG.info(format("Wrote %s mutations to table", numMutations));
    }

    /**
     * Executes task, ingesting and indexing the configured delimited file into Accumulo
     *
     * @return 0 if successful, non-zero if less than successful
     * @throws Exception
     */
    public int exec()
            throws Exception
    {
        // Validate the parameters have been set
        int numErrors = checkParam(config, "config");
        numErrors += checkParam(schema, "schemaName");
        numErrors += checkParam(tableName, "tableName");
        numErrors += checkParam(path, "path");
        if (numErrors > 0) {
            return 1;
        }

        this.init();

        // Get the accumulo table
        AccumuloTable table = this.getAccumuloTable();

        // Get the schema and other information to convert the line of text into a Row and then a
        // Mutation
        RowSchema schema = RowSchema.fromColumns(table.getColumns());
        int rowIdOrdinal = table.getRowIdOrdinal();
        List<AccumuloColumnHandle> columns = table.getColumns();
        AccumuloRowSerializer serializer = table.getSerializerInstance();

        FileSystem fs = FileSystem.get(new Configuration());

        if (!fs.exists(path)) {
            throw new FileNotFoundException(format("Path does not exist: %s", fs.makeQualified(path)));
        }

        List<Path> paths = new ArrayList<>();
        if (fs.isDirectory(path)) {
            for (FileStatus status : fs.listStatus(path)) {
                if (!status.isDirectory()) {
                    paths.add(status.getPath());
                }
            }

            if (paths.size() == 0) {
                throw new FileNotFoundException(format("Directory does not contain any files: %s", fs.makeQualified(path)));
            }
        }
        else {
            paths.add(path);
        }

        for (Path p : paths) {
            try (BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(p)))) {
                String line;
                while ((line = rdr.readLine()) != null) {
                    this.addMutation(AccumuloPageSink.toMutation(Row.fromString(schema, line, delimiter),
                            rowIdOrdinal, columns, serializer));
                }
            }
        }

        // Flush and close the writer
        this.flush();
        this.close();
        return 0;
    }

    @Override
    public int run(AccumuloConfig config, CommandLine cmd)
            throws Exception
    {
        this.setConfig(config);
        this.setSchema(cmd.getOptionValue(SCHEMA_OPT));
        this.setTableName(cmd.getOptionValue(TABLE_OPT));
        this.setPath(new Path(cmd.getOptionValue(PATH_OPT)));
        if (cmd.hasOption(DELIMITER_OPT)) {
            String del = cmd.getOptionValue(DELIMITER_OPT);
            if (del.length() != 1) {
                throw new InvalidParameterException("Delimiter must be one character");
            }

            this.setDelimiter(del.charAt(0));
        }
        return this.exec();
    }

    /**
     * Sets the {@link AccumuloConfig} to use for the task
     *
     * @param config Accumulo config
     */
    public void setConfig(AccumuloConfig config)
    {
        this.config = config;
    }

    /**
     * Sets the Presto schema of the table
     *
     * @param schema Presto schema
     */
    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * Sets the Presto table name (no schema)
     *
     * @param tableName Presto table name
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * Sets the Hadoop Path for the file or directory to be ingested. Not required if using this programatically.
     *
     * @param path Hadoop path
     */
    public void setPath(Path path)
    {
        this.path = path;
    }

    /**
     * Sets the delimiter to split the line of delimited data in the file. Default is a comma. Not
     * required if using this programatically.
     *
     * @param delimiter Delimiter of the data file
     */
    public void setDelimiter(char delimiter)
    {
        this.delimiter = delimiter;
    }

    /**
     * Sets the BatchWriterConfig to use for the writer and indexer. Default is the default
     * BatchWriterConfig.
     *
     * @param bwc BatchWriterConfig to use
     */
    public void setBatchWriterConfig(BatchWriterConfig bwc)
    {
        this.bwc = bwc;
    }

    @Override
    public String getTaskName()
    {
        return TASK_NAME;
    }

    @Override
    public String getDescription()
    {
        return DESCRIPTION;
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions()
    {
        Options opts = new Options();
        opts.addOption(
                OptionBuilder.withLongOpt("schema").withDescription("Schema name for the table")
                        .hasArg().isRequired().create(SCHEMA_OPT));
        opts.addOption(OptionBuilder.withLongOpt("table").withDescription("Table name").hasArg()
                .isRequired().create(TABLE_OPT));
        opts.addOption(OptionBuilder.withLongOpt("file")
                .withDescription("Hadoop Path to a file or directory of files to write to Accumulo").hasArg().isRequired()
                .create(PATH_OPT));
        opts.addOption(OptionBuilder.withLongOpt("delimiter")
                .withDescription("Delimiter of the file.  Default is a comma").hasArg()
                .create(DELIMITER_OPT));
        return opts;
    }
}
