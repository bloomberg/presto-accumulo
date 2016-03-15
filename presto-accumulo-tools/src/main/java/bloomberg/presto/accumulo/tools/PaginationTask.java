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
import bloomberg.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.cli.AlignedTablePrinter;
import com.facebook.presto.jdbc.PrestoConnection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static java.lang.String.format;

public class PaginationTask
        implements Task
{
    public static final String TASK_NAME = "pagination";

    private static final Logger LOG = Logger.getLogger(PaginationTask.class);
    private static final String JDBC_DRIVER = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String SCHEME = "jdbc:presto://";
    private static final String CATALOG = "accumulo";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    // User-specified configuration items
    private AccumuloConfig config;
    private String host;
    private Integer port;
    private Boolean rangeSplitsEnabled;
    private Boolean indexEnabled;
    private Boolean indexMetricsEnabled;
    private Double indexThreshold;
    private Double lowestCardinalityThreshold;
    private String query;
    private String[] columns = null;

    private String tmpTableName;
    private PrestoConnection conn;

    private long min = 0;
    private long max = 0;
    private long maxOffset = 0;
    private long pageSize = 20;

    private static final String TMP_TABLE = "tmp.table";
    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String TMP_COLUMN_MAPPING = "tmp.column.mapping";
    private static final String SUBQUERY_COLUMNS = "subquery.columns";
    private static final String USER_QUERY = "user.query";

    // @formatter:off
    private final String createTableTemplate = StringUtils.join(new String[] {
        "CREATE TABLE ${" + TMP_TABLE + "}",
        "WITH",
        "(",
        "    column_mapping = '${" + TMP_COLUMN_MAPPING + "}'",
        ")",
        "AS",
        "SELECT ",
        "    row_number() OVER (PARTITION BY t.groupby) AS offset, ${" + SUBQUERY_COLUMNS + "}",
        "FROM",
        "(",
        "    ${" + USER_QUERY + "}",
        ") t"
    }, '\n');

    private final String selectQueryTemplate = 
            "SELECT ${" + SUBQUERY_COLUMNS + "} " +
            "FROM ${" + TMP_TABLE + "} " +
            "WHERE offset > ${" + MIN + "} AND OFFSET <= ${" + MAX + "}";    
    // @formatter:on

    public int exec()
            throws Exception
    {
        try {
            // First, run the query to create the temporary table for pagination
            if (runQuery() == 0) {
                ResultSet rs;
                // Then, begin the pagination
                AlignedTablePrinter table = new AlignedTablePrinter(Arrays.asList(columns),
                        new PrintWriter(System.out));

                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                while (this.hasNext()) {
                    rs = this.next();
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();
                    List<List<?>> rows = new ArrayList<>();
                    while (rs.next()) {
                        List<String> row = new ArrayList<>();
                        rows.add(row);
                        for (int i = 1; i <= columnsNumber; i++) {
                            row.add(rs.getString(i));
                        }
                    }

                    table.printRows(rows, false);
                    System.out.println(
                            "--------- Press enter to move onto the next page, or enter 'q' to quit ---------");

                    // wait for user input
                    String line = in.readLine();
                    if (line.equals("q")) {
                        break;
                    }
                }
            }
        }
        finally

        {
            // And finally, drop all stuff
            cleanup();
        }
        return 0;

    }

    public int runQuery()
            throws SQLException
    {
        int numErrors = 0;
        numErrors += checkParam(config, "config");
        numErrors += checkParam(host, "host");
        numErrors += checkParam(port, "port");
        numErrors += checkParam(query, "query");
        numErrors += checkParam(columns, "columns");

        if (numErrors > 0) {
            return 1;
        }
        
        // Clean up any previously run queries in the event the user did not call it explicitly
        cleanup();

        String dbUrl = String.format("%s%s:%d/%s", SCHEME, host, port, CATALOG);

        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", "root");
        conn = (PrestoConnection) DriverManager.getConnection(dbUrl, jdbcProps);
        conn.setCatalog(CATALOG);
        setSessionProperties(conn);

        // randomly generate a table name
        tmpTableName =
                "accumulo.pagination.tmp_" + UUID.randomUUID().toString().replaceAll("\\W", "");

        // Build the column mapping based on
        StringBuilder columnMapping = new StringBuilder();
        for (String col : columns) {
            columnMapping.append(col).append(":f:")
                    .append(UUID.randomUUID().toString().substring(0, 8)).append(',');
        }
        columnMapping.deleteCharAt(columnMapping.length() - 1);

        StringBuilder queryWithGroupBy = new StringBuilder("SELECT 0 AS groupby, ");
        queryWithGroupBy.append(query.substring(query.indexOf("SELECT ") + 7));

        // Substitute the parameters to generate the create table query
        Map<String, String> queryProps = new HashMap<>();
        queryProps.put(TMP_TABLE, tmpTableName);
        queryProps.put(TMP_COLUMN_MAPPING, columnMapping.toString());
        queryProps.put(SUBQUERY_COLUMNS, StringUtils.join(columns, ','));
        queryProps.put(USER_QUERY, queryWithGroupBy.toString());

        StrSubstitutor sub = new StrSubstitutor(queryProps);
        String createTableQuery = sub.replace(createTableTemplate);
        LOG.info(format("Executing query to create temporary table:\n%s", createTableQuery));
        Statement stmt = conn.createStatement();
        stmt.execute(createTableQuery);

        stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery("SELECT MAX(offset) FROM " + tmpTableName);
        results.next();
        maxOffset = results.getLong(1);

        LOG.info(format("Query has %d results", maxOffset));

        return 0;
    }

    private ResultSet prevResultSet = null;

    public boolean hasNext()
    {
        return max < maxOffset;
    }

    public ResultSet next()
            throws SQLException
    {
        if (max < maxOffset) {
            // get min and max values to get for this result set, update current offset
            min = max;
            max = Math.min(max + pageSize, maxOffset);
        }

        Map<String, Object> queryProps = new HashMap<>();
        queryProps.put(SUBQUERY_COLUMNS, StringUtils.join(columns, ','));
        queryProps.put(TMP_TABLE, tmpTableName);
        queryProps.put(MIN, min);
        queryProps.put(MAX, max);

        StrSubstitutor sub = new StrSubstitutor(queryProps);
        String nextQuery = sub.replace(selectQueryTemplate);

        LOG.info(format("Executing %s", nextQuery));
        Statement stmt = conn.createStatement();
        prevResultSet = stmt.executeQuery(nextQuery);
        return prevResultSet;
    }

    public ResultSet previous()
            throws SQLException
    {
        // get min and max values to get for this result set, update current offset
        min = Math.max(min - pageSize, 0);
        max = Math.max(max - pageSize, pageSize);

        Map<String, Object> queryProps = new HashMap<>();
        queryProps.put(SUBQUERY_COLUMNS, StringUtils.join(columns, ','));
        queryProps.put(TMP_TABLE, tmpTableName);
        queryProps.put(MIN, min);
        queryProps.put(MAX, max);

        StrSubstitutor sub = new StrSubstitutor(queryProps);
        String prevQuery = sub.replace(selectQueryTemplate);

        LOG.info(format("Executing %s", prevQuery));
        Statement stmt = conn.createStatement();
        prevResultSet = stmt.executeQuery(prevQuery);
        return prevResultSet;
    }

    public void cleanup()
            throws SQLException
    {
        if (tmpTableName == null) {
            return;
        }

        String dropTable = "DROP TABLE " + tmpTableName;
        LOG.info(format("Executing %s", dropTable));
        conn.createStatement().execute(dropTable);
        tmpTableName = null;
        min = 0;
        max = 0;
        maxOffset = 0;
    }

    public void setConfig(AccumuloConfig config)
    {
        this.config = config;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public void setPort(Integer port)
    {
        this.port = port;
    }

    public void setRangeSplitsEnabled(Boolean rangeSplitsEnabled)
    {
        this.rangeSplitsEnabled = rangeSplitsEnabled;
    }

    public void setIndexEnabled(Boolean indexEnabled)
    {
        this.indexEnabled = indexEnabled;
    }

    public void setIndexThreshold(Double indexThreshold)
    {
        this.indexThreshold = indexThreshold;
    }

    public void setIndexMetricsEnabled(Boolean indexMetricsEnabled)
    {
        this.indexMetricsEnabled = indexMetricsEnabled;
    }

    public void setLowestCardinalityThreshold(Double lowestCardinalityThreshold)
    {
        this.lowestCardinalityThreshold = lowestCardinalityThreshold;
    }

    public void setQuery(String query)
    {
        this.query = query.trim();
        if (this.query.endsWith(";")) {
            this.query = this.query.substring(0, this.query.length() - 1);
        }
    }

    public void setQueryColumnNames(String[] columns)
    {
        this.columns = columns;
    }

    private void setSessionProperties(PrestoConnection conn)
    {
        if (rangeSplitsEnabled != null) {
            conn.setSessionProperty(AccumuloSessionProperties.OPTIMIZE_RANGE_SPLITS_ENABLED,
                    Boolean.toString(rangeSplitsEnabled));
        }

        if (indexEnabled != null) {
            conn.setSessionProperty(AccumuloSessionProperties.OPTIMIZE_INDEX_ENABLED,
                    Boolean.toString(indexEnabled));
        }

        if (indexThreshold != null) {
            conn.setSessionProperty(AccumuloSessionProperties.INDEX_THRESHOLD,
                    Double.toString(indexThreshold));
        }

        if (lowestCardinalityThreshold != null) {
            conn.setSessionProperty(AccumuloSessionProperties.INDEX_LOWEST_CARDINALITY_THRESHOLD,
                    Double.toString(lowestCardinalityThreshold));
        }

        if (indexMetricsEnabled != null) {
            conn.setSessionProperty(AccumuloSessionProperties.INDEX_METRICS_ENABLED,
                    Boolean.toString(indexMetricsEnabled));
        }
    }

    public void setPageSize(int size)
    {
        this.pageSize = size;
    }

    public long getPageSize()
    {
        return this.pageSize;
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
        this.setHost(args[0]);
        this.setPort(Integer.parseInt(args[1]));
        this.setQuery(IOUtils.toString(new FileInputStream(args[2])));
        this.setQueryColumnNames(args[3].split(","));
        if (args.length == 5) {
            this.setPageSize(Integer.parseInt(args[4]));
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
        return "\t" + TASK_NAME
                + " <presto.host> <presto.port> <query.file> <query.return.columns> [results.per.page]";
    }

    @Override
    public boolean isNumArgsOk(int i)
    {
        return i == 4 || i == 5;
    }
}
