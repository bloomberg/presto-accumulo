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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.configuration.ConfigurationException;

import java.io.File;

import static java.lang.String.format;

public class Main
{
    public static void main(String[] args)
            throws ConfigurationException, AccumuloException, AccumuloSecurityException
    {
        String prestoHome = System.getenv("PRESTO_HOME");
        if (prestoHome == null) {
            System.err.println("PRESTO_HOME is not set");
            System.exit(1);
        }

        if (args.length < 1) {
            System.out.println("Usage: java -jar <jarfile> <tool> [args]");
            System.out.println("Available tools:");
            System.out.println("\taddcolumn <schema.name> <table.name> <presto.name> <presto.type> "
                    + "<column.family> <column.qualifier> <indexed> [zero.based.ordinal]");
            System.exit(0);
        }

        AccumuloConfig config =
                AccumuloConfig.from(new File(prestoHome, "etc/catalog/accumulo.properties"));
        AccumuloClient client = new AccumuloClient(new AccumuloConnectorId("accumulo"), config);

        if (args[0].equals("addcolumn") && args.length >= 8) {
            String schema = args[1];
            String tablename = args[2];
            String name = args[3];
            Type type = new TypeRegistry().getType(TypeSignature.parseTypeSignature(args[4]));
            String family = args[5];
            String qualifier = args[6];
            boolean indexed = Boolean.parseBoolean(args[7]);

            String comment =
                    String.format("Accumulo column %s:%s. Indexed: %b", family, qualifier, indexed);

            AccumuloTable table = client.getTable(new SchemaTableName(schema, tablename));
            if (table == null) {
                System.err.println(format("Metadata for table %s.%s not found.  Does it exist?",
                        schema, tablename));
                System.exit(1);
            }

            int ordinal = args.length == 9 ? Integer.parseInt(args[8]) : table.getColumns().size();

            AccumuloColumnHandle column = new AccumuloColumnHandle("accumulo", name, family,
                    qualifier, type, ordinal, comment, indexed);
            client.addColumn(table, column);
        }
        else {
            System.err.println("Unknown tool " + args[0] + " or invalid number of parameters");
            System.out.println("Usage: java -jar <jarfile> <tool> [args]");
            System.out.println("Available tools:");
            System.out.println("\taddcolumn <schema.name> <table.name> <presto.name> <presto.type> "
                    + "<column.family> <column.qualifier> <indexed> [zero.based.ordinal]");
            System.exit(1);
        }
    }
}
