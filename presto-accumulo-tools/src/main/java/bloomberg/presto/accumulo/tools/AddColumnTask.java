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

import bloomberg.presto.accumulo.AccumuloClient;
import bloomberg.presto.accumulo.AccumuloConnectorId;
import bloomberg.presto.accumulo.conf.AccumuloConfig;
import bloomberg.presto.accumulo.metadata.AccumuloTable;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;

import static java.lang.String.format;

public class AddColumnTask
        implements Task
{
    public static final String TASK_NAME = "addcolumn";

    private AccumuloConfig config;
    private String schema;
    private String tableName;
    private String prestoName;
    private String family;
    private String qualifier;
    private Type type = null;
    private Boolean indexed;
    private Integer ordinal;

    public void setConfig(AccumuloConfig config)
    {
        this.config = config;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public void setPrestoName(String prestoName)
    {
        this.prestoName = prestoName;
    }

    public void setFamily(String family)
    {
        this.family = family;
    }

    public void setQualifier(String qualifier)
    {
        this.qualifier = qualifier;
    }

    public void setType(Type type)
    {
        this.type = type;
    }

    public void setIndexed(boolean indexed)
    {
        this.indexed = indexed;
    }

    public void setOrdinal(int ordinal)
    {
        this.ordinal = ordinal;
    }

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

        if (numErrors > 0) {
            return 1;
        }

        String comment =
                String.format("Accumulo column %s:%s. Indexed: %b", family, qualifier, indexed);

        AccumuloClient client = new AccumuloClient(new AccumuloConnectorId("accumulo"), config);
        AccumuloTable table = client.getTable(new SchemaTableName(schema, tableName));
        if (table == null) {
            System.err.println(format("Metadata for table %s.%s not found.  Does it exist?", schema,
                    tableName));
            return 1;
        }

        AccumuloColumnHandle column = new AccumuloColumnHandle("accumulo", prestoName, family,
                qualifier, type, ordinal, comment, indexed);
        client.addColumn(table, column);
        System.out.println(format("Created column %s", column));
        return 0;
    }

    private int checkParam(Object o, String name)
    {
        if (o == null) {
            System.err.println(format("Parameter %s is not set", name));
            return 1;
        }
        return 0;
    }

    @Override
    public int run(AccumuloConfig config, String[] args)
            throws Exception
    {
        this.setConfig(config);
        this.setSchema(args[0]);
        this.setTableName(args[1]);
        this.setPrestoName(args[2]);
        this.setType(new TypeRegistry().getType(TypeSignature.parseTypeSignature(args[3])));
        this.setFamily(args[4]);
        this.setQualifier(args[5]);
        this.setIndexed(Boolean.parseBoolean(args[6]));
        if (args.length == 8) {
            this.setOrdinal(Integer.parseInt(args[7]));
        }

        return this.exec();
    }

    @Override
    public String getTaskName()
    {
        return TASK_NAME;
    }

    @Override
    public String getHelp()
    {
        return "\taddcolumn <schema.name> <table.name> <presto.name> <presto.type> "
                + "<column.family> <column.qualifier> <indexed> [zero.based.ordinal]";
    }

    @Override
    public boolean isNumArgsOk(int i)
    {
        return i == 7 || i == 8;
    }
}
