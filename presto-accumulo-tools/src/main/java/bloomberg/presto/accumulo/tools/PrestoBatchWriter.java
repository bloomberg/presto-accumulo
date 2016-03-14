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
package bloomberg.presto.accumulo.tools;

import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.index.Indexer;
import bloomberg.presto.accumulo.io.AccumuloPageSink;
import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.metadata.AccumuloTable;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.model.Row;
import bloomberg.presto.accumulo.model.RowSchema;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import static java.lang.String.format;

public class PrestoBatchWriter
        implements Task
{
    public static final String TASK_NAME = "batchwriter";
    private static final Logger LOG = Logger.getLogger(PrestoBatchWriter.class);

    // User-configured values
    private AccumuloConfig config;
    private String schemaName;
    private String tableName;
    private File file;
    private BatchWriterConfig bwc;

    // Created by task
    private int numMutations;
    private AccumuloTable table = null;
    private BatchWriter writer = null;
    private Indexer indexer = null;

    public PrestoBatchWriter()
    {}

    public PrestoBatchWriter(AccumuloConfig config, String schema, String table)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        this(config, schema, table, null);
    }

    public PrestoBatchWriter(AccumuloConfig config, String schema, String table,
            BatchWriterConfig conf)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        this.setConfig(config);
        this.setSchemaName(schema);
        this.setTableName(table);
        this.init();
    }

    public void init()
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        Instance inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        Connector conn =
                inst.getConnector(config.getUsername(), new PasswordToken(config.getPassword()));

        AccumuloMetadataManager manager = config.getMetadataManager();
        this.table = manager.getTable(new SchemaTableName(schemaName, tableName));

        final Authorizations auths;
        if (table.getScanAuthorizations() != null) {
            auths = new Authorizations(table.getScanAuthorizations().split(","));
        }
        else {
            auths = conn.securityOperations().getUserAuthorizations(config.getUsername());
        }

        if (bwc == null) {
            bwc = new BatchWriterConfig();
        }

        writer = conn.createBatchWriter(table.getFullTableName(), bwc);
        if (table.isIndexed()) {
            indexer = new Indexer(conn, auths, table, bwc);
            LOG.info(format("Created writer and indexer for table %s", table.getFullTableName()));
        }
        else {
            LOG.info(format("Created writer for table %s -- table is not indexed",
                    table.getFullTableName()));
        }
    }

    public void addMutation(Mutation m)
            throws MutationsRejectedException
    {
        writer.addMutation(m);
        if (indexer != null) {
            indexer.index(m);
        }
        ++numMutations;
    }

    public void addMutations(Iterable<Mutation> m)
            throws MutationsRejectedException
    {
        writer.addMutations(m);
        if (indexer != null) {
            indexer.index(m);
        }
    }

    public void flush()
            throws MutationsRejectedException
    {
        writer.flush();
        if (indexer != null) {
            indexer.flush();
        }
    }

    public void close()
            throws MutationsRejectedException
    {
        writer.close();
        if (indexer != null) {
            indexer.close();
        }

        LOG.info(format("Wrote %s mutations to table", numMutations));
    }

    public void setConfig(AccumuloConfig config)
    {
        this.config = config;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public void setFile(File file)
    {
        this.file = file;
    }

    /**
     * Gets the {@link AccumuloTable} object. Only valid after {@link PrestoBatchWriter#init} is
     * called.
     * 
     * @return AccumuloTable, or null if init was not called
     */
    public AccumuloTable getAccumuloTable()
    {
        return this.table;
    }

    @Override
    public int run(AccumuloConfig config, String[] args)
            throws Exception
    {
        this.setConfig(config);
        this.setSchemaName(args[0]);
        this.setTableName(args[1]);
        this.setFile(new File(args[2]));
        return this.exec();
    }

    public int exec()
            throws Exception
    {
        int numErrors = checkParam(config, "config");
        numErrors += checkParam(schemaName, "schemaName");
        numErrors += checkParam(tableName, "tableName");
        numErrors += checkParam(file, "file");
        if (numErrors > 0) {
            return 1;
        }

        this.init();

        AccumuloTable table = this.getAccumuloTable();

        RowSchema schema = RowSchema.fromColumns(table.getColumns());
        int rowIdOrdinal = table.getRowIdOrdinal();
        List<AccumuloColumnHandle> columns = table.getColumns();
        AccumuloRowSerializer serializer = table.getSerializerInstance();

        BufferedReader rdr = new BufferedReader(new FileReader(file));
        String line;
        while ((line = rdr.readLine()) != null) {
            this.addMutation(AccumuloPageSink.toMutation(Row.fromString(schema, line, ','),
                    rowIdOrdinal, columns, serializer));
        }

        this.flush();
        this.close();
        rdr.close();
        return 0;
    }

    @Override
    public String getTaskName()
    {
        return TASK_NAME;
    }

    @Override
    public String getHelp()
    {
        return "\t" + TASK_NAME + " <schema> <table> <file>";
    }

    @Override
    public boolean isNumArgsOk(int i)
    {
        return i == 3;
    }

    private int checkParam(Object o, String name)
    {
        if (o == null) {
            System.err.println(format("Parameter %s is not set", name));
            return 1;
        }
        return 0;
    }
}
