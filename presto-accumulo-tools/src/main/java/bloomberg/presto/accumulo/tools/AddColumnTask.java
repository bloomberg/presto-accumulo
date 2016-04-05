/**
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
package bloomberg.presto.accumulo.tools;

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.AccumuloConnectorId;
import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.metadata.AccumuloTable;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

/**
 * This task is used to add an additional column to an existing Presto table. We are unable to use
 * SQL to do this because additional metadata is required by the connector to operate.
 */
public class AddColumnTask
        extends Task
{
    public static final String TASK_NAME = "addcolumn";
    public static final String DESCRIPTION =
            "Adds a column to an existing Presto table via metadata manipulation";

    // Command line options
    private static final char SCHEMA_OPT = 's';
    private static final char TABLE_OPT = 't';
    private static final char NAME_OPT = 'n';
    private static final char TYPE_OPT = 'p';
    private static final char FAMILY_OPT = 'f';
    private static final char QUALIFIER_OPT = 'q';
    private static final char INDEXED_OPT = 'i';
    private static final char ORDINAL_OPT = 'o';

    private AccumuloConfig config;
    private String schema;
    private String tableName;
    private String prestoName;
    private String family;
    private String qualifier;
    private Type type = null;
    private Boolean indexed;
    private Integer ordinal;

    /**
     * Executes the task to add a new column, validating all arguments are set.
     *
     * @return 0 if the task is successful, false otherwise
     * @throws Exception
     *             If something bad happens
     */
    public int exec()
            throws Exception
    {
        int numErrors = 0;
        numErrors += checkParam(config, "config");
        numErrors += checkParam(schema, "schema");
        numErrors += checkParam(tableName, "tableName");
        numErrors += checkParam(prestoName, "prestoName");
        numErrors += checkParam(family, "family");
        numErrors += checkParam(qualifier, "qualifier");
        numErrors += checkParam(type, "type");
        numErrors += checkParam(indexed, "indexed");

        // Return 1 if a required parameter has not been set
        if (numErrors > 0) {
            return 1;
        }

        // Fetch the metadata from the client
        AccumuloClient client = new AccumuloClient(new AccumuloConnectorId("accumulo"), config);
        AccumuloTable table = client.getTable(new SchemaTableName(schema, tableName));
        if (table == null) {
            System.err.println(format("Metadata for table %s.%s not found.  Does it exist?", schema,
                    tableName));
            return 1;
        }

        // If the ordinal is not set, put it at the end of the row
        if (ordinal == null) {
            ordinal = table.getColumns().size();
        }

        // Create the comment string for the column
        String comment =
                String.format("Accumulo column %s:%s. Indexed: %b", family, qualifier, indexed);

        // Create our column and add it via the client
        AccumuloColumnHandle column = new AccumuloColumnHandle("accumulo", prestoName, family,
                qualifier, type, ordinal, comment, indexed);
        client.addColumn(table, column);
        System.out.println(format("Created column %s", column));

        return 0;
    }

    /**
     * Runs the task using the given config and parsed command line options
     *
     * @param config
     *            Accumulo configuration
     * @param cmd
     *            Parsed command line
     * @return 0 if the task was successful, false otherwise
     * @throws Exception
     */
    @Override
    public int run(AccumuloConfig config, CommandLine cmd)
            throws Exception
    {
        this.setConfig(config);
        this.setSchema(cmd.getOptionValue(SCHEMA_OPT));
        this.setTableName(cmd.getOptionValue(TABLE_OPT));
        this.setPrestoName(cmd.getOptionValue(NAME_OPT));
        this.setType(new TypeRegistry()
                .getType(TypeSignature.parseTypeSignature(cmd.getOptionValue(TYPE_OPT))));
        this.setFamily(cmd.getOptionValue(FAMILY_OPT));
        this.setQualifier(cmd.getOptionValue(QUALIFIER_OPT));
        this.setIndexed(cmd.hasOption(INDEXED_OPT));
        if (cmd.hasOption(ORDINAL_OPT)) {
            this.setOrdinal(Integer.parseInt(cmd.getOptionValue(ORDINAL_OPT)));
        }
        return this.exec();
    }

    /**
     * Sets the {@link AccumuloConfig} to use for the task
     *
     * @param config
     *            Accumulo config
     */
    public void setConfig(AccumuloConfig config)
    {
        this.config = config;
    }

    /**
     * Sets the Presto schema of the table
     *
     * @param schema
     *            Presto schema
     */
    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    /**
     * Sets the Presto table name (no schema)
     *
     * @param tableName
     *            Presto table name
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * Sets the Presto column name to be added
     *
     * @param prestoName
     *            New column name
     */
    public void setPrestoName(String prestoName)
    {
        this.prestoName = prestoName;
    }

    /**
     * Sets the Accumulo column family for the mapping
     *
     * @param family
     *            Column family
     */
    public void setFamily(String family)
    {
        this.family = family;
    }

    /**
     * Sets the Accumulo column qualifier for the mapping
     *
     * @param qualifier
     *            Column family
     */
    public void setQualifier(String qualifier)
    {
        this.qualifier = qualifier;
    }

    /**
     * Sets the Presto data type
     *
     * @param type
     *            Presto type
     */
    public void setType(Type type)
    {
        this.type = type;
    }

    /**
     * Sets a Boolean value indicating whether or not this column is indexed
     *
     * @param indexed
     *            True if indexed, fase otherwise
     */
    public void setIndexed(boolean indexed)
    {
        this.indexed = indexed;
    }

    /**
     * Sets the ordinal of the column to be inserted into the row definition. Default is at the end
     * of the row if this is not set.
     *
     * @param ordinal
     *            Ordinal of the row
     */
    public void setOrdinal(int ordinal)
    {
        this.ordinal = ordinal;
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
        opts.addOption(
                OptionBuilder.withLongOpt("name").withDescription("Presto column name to add")
                        .hasArg().isRequired().create(NAME_OPT));
        opts.addOption(
                OptionBuilder.withLongOpt("type").withDescription("Presto type of the new column")
                        .hasArg().isRequired().create(TYPE_OPT));
        opts.addOption(OptionBuilder.withLongOpt("family").withDescription("Column family mapping")
                .hasArg().isRequired().create(FAMILY_OPT));
        opts.addOption(
                OptionBuilder.withLongOpt("qualifier").withDescription("Column qualifier mapping")
                        .hasArg().isRequired().create(QUALIFIER_OPT));
        opts.addOption(OptionBuilder.withLongOpt("indexed")
                .withDescription("Flag to set whether or not the column is indexed")
                .create(INDEXED_OPT));
        opts.addOption(OptionBuilder.withLongOpt("ordinal")
                .withDescription(
                        "Optional ordinal to insert the column.  Default is at the end of the row")
                .hasArg().create(ORDINAL_OPT));
        return opts;
    }
}
