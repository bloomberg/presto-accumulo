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
package com.facebook.presto.accumulo.tools;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.cli.AlignedTablePrinter;
import com.facebook.presto.jdbc.PrestoConnection;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.log4j.Logger;

import javax.activity.InvalidActivityException;

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

/**
 * This task is used to execute a query and paginate results. It is mainly intended to be used
 * programatically (because you can just use the presto-cli to query and paginate results), but
 * it can be executed at the command line.
 */
public class PaginationTask
        extends Task
{
    public static final String TASK_NAME = "pagination";
    public static final String DESCRIPTION =
            "Queries a Presto table for rows of data, interactively displaying the results in pages";

    private static final Logger LOG = Logger.getLogger(PaginationTask.class);

    // Options
    private static final char HOST_OPT = 'h';
    private static final char PORT_OPT = 'p';
    private static final char QUERY_FILE_OPT = 'f';
    private static final char COLUMNS_OPT = 'c';
    private static final char PAGE_SIZE_OPT = 's';

    // JDBC constants
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

    // StrSubstitutor values
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
            "WHERE offset > ${" + MIN + "} AND offset <= ${" + MAX + "}";
    // @formatter:on

    // User-specified configuration items
    private AccumuloConfig config;
    private String host;
    private Integer port;
    private Boolean localityEnabled;
    private Boolean rangeSplitsEnabled;
    private Boolean indexEnabled;
    private Boolean indexMetricsEnabled;
    private Double indexThreshold;
    private Double lowestCardinalityThreshold;
    private Integer rowsPerSplit;
    private String query;
    private String[] columns = null;

    private String tmpTableName;
    private PrestoConnection conn;

    // Keep track of the min/max offset
    private long min = 0;
    private long max = 0;

    // Maximum available offset for querying rows of data
    private long maxOffset = 0;

    // Default page size for moving between the tables
    private long pageSize = 20;

    /**
     * This function executes the query, paging results to stdout
     *
     * @return 0 if successful, non-zero otherwise
     * @throws Exception
     */
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

                // Open up a reader to get user input
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

                // While there are rows to print
                while (this.hasNext()) {
                    // Get the next batch of rows
                    rs = this.next();

                    // Build a list of rows
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

                    // Print the rows!
                    table.printRows(rows, false);

                    // If there is still data to present, wait for user input, going to the next
                    // page or exiting if 'q' is entered
                    if (this.hasNext()) {
                        System.out.println(
                                "--------- Press enter to move onto the next page, or enter 'q' to quit ---------");
                        String line = in.readLine();
                        if (line.equals("q")) {
                            break;
                        }
                    }
                }
            }
        }
        finally {
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

        // Open JDBC connection
        String dbUrl = String.format("%s%s:%d/%s", SCHEME, host, port, CATALOG);
        Properties jdbcProps = new Properties();
        jdbcProps.setProperty("user", "root");
        conn = (PrestoConnection) DriverManager.getConnection(dbUrl, jdbcProps);
        conn.setCatalog(CATALOG);
        setSessionProperties(conn);

        // Randomly generate a table name as a local variable
        String tmpTable =
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
        queryProps.put(TMP_TABLE, tmpTable);
        queryProps.put(TMP_COLUMN_MAPPING, columnMapping.toString());
        queryProps.put(SUBQUERY_COLUMNS, StringUtils.join(columns, ','));
        queryProps.put(USER_QUERY, queryWithGroupBy.toString());

        // Execute the create table query
        StrSubstitutor sub = new StrSubstitutor(queryProps);
        String createTableQuery = sub.replace(createTableTemplate);
        LOG.info(format("Executing query to create temporary table:\n%s", createTableQuery));
        Statement stmt = conn.createStatement();
        stmt.execute(createTableQuery);

        // Execute the query to get the max offset i.e. number of rows from the user query
        stmt = conn.createStatement();
        ResultSet results = stmt.executeQuery("SELECT MAX(offset) FROM " + tmpTable);
        results.next();
        maxOffset = results.getLong(1);
        LOG.info(format("Query has %d results", maxOffset));

        // Set the temp table name now that we have made it through the gauntlet
        this.tmpTableName = tmpTable;
        return 0;
    }

    /**
     * Gets a Boolean value indicating whether or not there are rows left to be paginated
     *
     * @return True if there are more rows, false otherwise
     */
    public boolean hasNext()
    {
        return max < maxOffset;
    }

    /**
     * Queries the temporary table, retrieving the next page of results
     *
     * @return Next page's ResultSet
     * @throws SQLException
     */
    public ResultSet next()
            throws SQLException
    {
        if (max < maxOffset) {
            // get min and max values to get for this result set, update current offset
            min = max;
            max = Math.min(max + pageSize, maxOffset);
        }
        // else, just use the last min/max and run the query again
        return getRows(min, max);
    }

    /**
     * Queries the temporary table, retrieving the previous page of results
     *
     * @return Previous page's ResultSet
     * @throws SQLException If an error occurs issuing the query
     */
    public ResultSet previous()
            throws SQLException
    {
        // TODO Fix when you hit the last page and then go back -- messes up the page
        // get min and max values to get for this result set, update current offset
        min = Math.max(min - pageSize, 0);
        max = Math.max(max - pageSize, pageSize);
        return getRows(min, max);
    }

    /**
     * Queries the temporary table for the rows of data from [min, max)
     *
     * @param min Minimum value of the offset to be retrieved, inclusive
     * @param max Maximum value of the offset to be retrieved, exclusive
     * @return ResultSet of the rows between the given offset
     * @throws SQLException If an error occurs issuing the query
     */
    public ResultSet getRows(long min, long max)
            throws SQLException
    {
        Map<String, Object> queryProps = new HashMap<>();
        queryProps.put(SUBQUERY_COLUMNS, StringUtils.join(columns, ','));
        queryProps.put(TMP_TABLE, tmpTableName);
        queryProps.put(MIN, min);
        queryProps.put(MAX, max);

        StrSubstitutor sub = new StrSubstitutor(queryProps);
        String prevQuery = sub.replace(selectQueryTemplate);

        LOG.info(format("Executing %s", prevQuery));
        return conn.createStatement().executeQuery(prevQuery);
    }

    /**
     * Cleans up the state of the PaginationTask so it may be used again. Failure to call this
     * method will result in state temporary tables in Accumulo. Nobody wants that.
     */
    public void cleanup()
    {
        if (tmpTableName == null) {
            return;
        }

        try {
            String dropTable = "DROP TABLE " + tmpTableName;
            LOG.info(format("Executing %s", dropTable));
            conn.createStatement().execute(dropTable);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        tmpTableName = null;
        min = 0;
        max = 0;
        maxOffset = 0;
    }

    @Override
    public int run(AccumuloConfig config, CommandLine cmd)
            throws Exception
    {
        this.setConfig(config);
        this.setHost(cmd.getOptionValue(HOST_OPT));
        this.setPort(Integer.parseInt(cmd.getOptionValue(PORT_OPT)));
        this.setQuery(IOUtils.toString(new FileInputStream(cmd.getOptionValue(QUERY_FILE_OPT))));
        this.setQueryColumnNames(cmd.getOptionValues(COLUMNS_OPT));
        this.setPageSize(Integer.parseInt(cmd.getOptionValue(PAGE_SIZE_OPT, "20")));
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
     * Sets the Presto host
     *
     * @param host Presto host
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * Sets the Presto port
     *
     * @param port Presto port
     */
    public void setPort(Integer port)
    {
        this.port = port;
    }

    /**
     * Sets the user query to be executed and paginated
     *
     * @param query Query string
     */
    public void setQuery(String query)
    {
        // Trim the query and remove the semicolon, if any
        this.query = query.trim();
        if (this.query.endsWith(";")) {
            this.query = this.query.substring(0, this.query.length() - 1);
        }
    }

    /**
     * Sets the column names, in order, to be retrieved from the user query
     *
     * @param columns Columns to be returned from the select query
     */
    public void setQueryColumnNames(String[] columns)
    {
        this.columns = columns;
    }

    /**
     * Sets the page size. Default is 20 rows per page.
     *
     * @param size New page size
     * @throws InvalidActivityException If an attempt to set the page size mid-query occurs. Call cleanup, then set the
     * page size
     */
    public void setPageSize(int size)
            throws InvalidActivityException
    {
        if (tmpTableName != null) {
            throw new InvalidActivityException("Cannot change the page size mid-query.  "
                    + "Cleanup first, then set the page size.");
        }
        this.pageSize = size;
    }

    /**
     * Gets the current page size
     *
     * @return Page size
     */
    public long getPageSize()
    {
        return this.pageSize;
    }

    /**
     * Sets the session parameter for enabling the index
     *
     * @param indexEnabled True to enable, false otherwise
     */
    public void setIndexEnabled(Boolean indexEnabled)
    {
        this.indexEnabled = indexEnabled;
    }

    /**
     * Sets the session parameter for enabling the use of the index metrics table
     *
     * @param indexMetricsEnabled True to enable, false otherwise
     */
    public void setIndexMetricsEnabled(Boolean indexMetricsEnabled)
    {
        this.indexMetricsEnabled = indexMetricsEnabled;
    }

    /**
     * Sets the session parameter for the index threhsold
     *
     * @param indexThreshold Threshold for using the index
     */
    public void setIndexThreshold(Double indexThreshold)
    {
        this.indexThreshold = indexThreshold;
    }

    /**
     * Sets the session parameter for enabling tablet locality lookups
     *
     * @param localityEnabled True to enable, false otherwise
     */
    public void setLocalityEnabled(Boolean localityEnabled)
    {
        this.localityEnabled = localityEnabled;
    }

    /**
     * Sets the session parameter for enabling range splits
     *
     * @param lowestCardinalityThreshold True to enable, false otherwise
     */
    public void setLowestCardinalityThreshold(Double lowestCardinalityThreshold)
    {
        this.lowestCardinalityThreshold = lowestCardinalityThreshold;
    }

    /**
     * Sets the session parameter for enabling range splits
     *
     * @param rangeSplitsEnabled True to enable, false otherwise
     */
    public void setRangeSplitsEnabled(Boolean rangeSplitsEnabled)
    {
        this.rangeSplitsEnabled = rangeSplitsEnabled;
    }

    /**
     * Sets the session parameter for the number of rows to be packed in a Presto split
     *
     * @param rowsPerSplit Number of rows per split
     */
    public void setRowsPerSplit(Integer rowsPerSplit)
    {
        this.rowsPerSplit = rowsPerSplit;
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
        opts.addOption(OptionBuilder.withLongOpt("host").withDescription("Presto server host name")
                .hasArg().isRequired().create(HOST_OPT));
        opts.addOption(OptionBuilder.withLongOpt("port").withDescription("Presto server port")
                .hasArg().isRequired().create(PORT_OPT));
        opts.addOption(OptionBuilder.withLongOpt("file")
                .withDescription("File containing SQL query to execute").hasArg().isRequired()
                .create(QUERY_FILE_OPT));
        opts.addOption(OptionBuilder.withLongOpt("columns")
                .withDescription("Columns returned from the SQL SELECT, in order").hasArgs()
                .isRequired().create(COLUMNS_OPT));
        opts.addOption(OptionBuilder.withLongOpt("size")
                .withDescription("Page size.  Default 20 rows per page").hasArg()
                .create(PAGE_SIZE_OPT));
        return opts;
    }

    private void setSessionProperties(PrestoConnection conn)
    {
        if (rangeSplitsEnabled != null) {
            conn.setSessionProperty("optimize_range_splits_enabled",
                    Boolean.toString(rangeSplitsEnabled));
        }

        if (indexEnabled != null) {
            conn.setSessionProperty("optimize_index_enabled",
                    Boolean.toString(indexEnabled));
        }

        if (localityEnabled != null) {
            conn.setSessionProperty("optimize_locality_enabled",
                    Boolean.toString(localityEnabled));
        }

        if (indexThreshold != null) {
            conn.setSessionProperty("index_threshold",
                    Double.toString(indexThreshold));
        }

        if (lowestCardinalityThreshold != null) {
            conn.setSessionProperty("index_lowest_cardinality_threshold",
                    Double.toString(lowestCardinalityThreshold));
        }

        if (rowsPerSplit != null) {
            conn.setSessionProperty("index_rows_per_split",
                    Integer.toString(rowsPerSplit));
        }

        if (indexMetricsEnabled != null) {
            conn.setSessionProperty("index_metrics_enabled",
                    Boolean.toString(indexMetricsEnabled));
        }
    }
}
