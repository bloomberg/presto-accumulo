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
package com.facebook.presto.accumulo.examples;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.tools.Task;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

import java.security.InvalidParameterException;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Pattern;

import static com.facebook.presto.accumulo.examples.Constants.CLERK_STR;
import static com.facebook.presto.accumulo.examples.Constants.COMMENT_STR;
import static com.facebook.presto.accumulo.examples.Constants.CUSTKEY_STR;
import static com.facebook.presto.accumulo.examples.Constants.DATA_TABLE;
import static com.facebook.presto.accumulo.examples.Constants.INDEX_TABLE;
import static com.facebook.presto.accumulo.examples.Constants.ORDERDATE_STR;
import static com.facebook.presto.accumulo.examples.Constants.ORDERPRIORITY_STR;
import static com.facebook.presto.accumulo.examples.Constants.ORDERSTATUS_STR;
import static com.facebook.presto.accumulo.examples.Constants.SHIPPRIORITY_STR;
import static com.facebook.presto.accumulo.examples.Constants.TOTALPRICE_STR;
import static com.facebook.presto.accumulo.examples.Constants.decode;
import static java.lang.String.format;

@SuppressWarnings("static-access")
public class TpcHClerkSearch
        extends Task
{
    private static final String TASK_NAME = "clerksearch";
    private static final String DESCRIPTION = "Example for searching the TPC-H table for all orders by a given clerk";
    private static final char CLERK_ID = 'c';

    private Pattern clerkRegex = Pattern.compile("Clerk#[0-9]{9}");

    @Override
    public int run(AccumuloConfig config, CommandLine cmd)
            throws Exception
    {
        String[] searchTerms = cmd.getOptionValues(CLERK_ID);

        ZooKeeperInstance inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        Connector conn = inst.getConnector(config.getUsername(), new PasswordToken(config.getPassword()));

        // Ensure both tables exists
        validateExists(conn, DATA_TABLE);
        validateExists(conn, INDEX_TABLE);

        long start = System.currentTimeMillis();

        // Create a scanner against the index table
        BatchScanner idxScanner = conn.createBatchScanner(INDEX_TABLE, new Authorizations(), 10);
        LinkedList<Range> searchRanges = new LinkedList<Range>();

        // Create a search Range from the command line args
        for (String searchTerm : searchTerms) {
            if (clerkRegex.matcher(searchTerm).matches()) {
                searchRanges.add(new Range(searchTerm));
            }
            else {
                throw new InvalidParameterException(format("Search term %s does not match regex Clerk#[0-9]{9}", searchTerm));
            }
        }

        // Set the search ranges for our scanner
        idxScanner.setRanges(searchRanges);

        // A list to hold all of the order IDs
        LinkedList<Range> orderIds = new LinkedList<Range>();
        String orderId;

        // Process all of the records returned by the batch scanner
        for (Map.Entry<Key, Value> record : idxScanner) {
            // Get the order ID and add it to the list of order IDs
            orderIds.add(new Range(record.getKey().getColumnQualifier()));
        }

        // Close the batch scanner
        idxScanner.close();

        // If clerkIDs is empty, log a message and return 0
        if (orderIds.isEmpty()) {
            System.out.println("Found no orders with the given Clerk ID(s)");
            return 0;
        }
        else {
            System.out.println(format("Searching data table for %d orders", orderIds.size()));
        }

        // Initialize the batch scanner to scan the data table with
        // the previously found order IDs as the ranges
        BatchScanner dataScanner = conn.createBatchScanner(DATA_TABLE, new Authorizations(), 10);
        dataScanner.setRanges(orderIds);
        dataScanner.addScanIterator(new IteratorSetting(1, WholeRowIterator.class));

        Text row = new Text(); // The row ID
        Text colQual = new Text(); // The column qualifier of the current record

        Long orderkey = null;
        Long custkey = null;
        String orderstatus = null;
        Double totalprice = null;
        Date orderdate = null;
        String orderpriority = null;
        String clerk = null;
        Long shippriority = null;
        String comment = null;

        int numTweets = 0;
        // Process all of the records returned by the batch scanner
        for (Map.Entry<Key, Value> entry : dataScanner) {
            entry.getKey().getRow(row);
            orderkey = decode(Long.class, row.getBytes(), row.getLength());
            SortedMap<Key, Value> rowMap = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
            for (Map.Entry<Key, Value> record : rowMap.entrySet()) {
                // Get the column qualifier from the record's key
                record.getKey().getColumnQualifier(colQual);

                switch (colQual.toString()) {
                    case CUSTKEY_STR:
                        custkey = decode(Long.class, record.getValue().get());
                        break;
                    case ORDERSTATUS_STR:
                        orderstatus = decode(String.class, record.getValue().get());
                        break;
                    case TOTALPRICE_STR:
                        totalprice = decode(Double.class, record.getValue().get());
                        break;
                    case ORDERDATE_STR:
                        orderdate = decode(Date.class, record.getValue().get());
                        break;
                    case ORDERPRIORITY_STR:
                        orderpriority = decode(String.class, record.getValue().get());
                        break;
                    case CLERK_STR:
                        clerk = decode(String.class, record.getValue().get());
                        break;
                    case SHIPPRIORITY_STR:
                        shippriority = decode(Long.class, record.getValue().get());
                        break;
                    case COMMENT_STR:
                        comment = decode(String.class, record.getValue().get());
                        break;
                    default:
                        throw new RuntimeException("Unknown column qualifier " + colQual);
                }
            }

            ++numTweets;
            // Write the screen name and text to stdout
            System.out.println(format("%d|%d|%s|%f|%s|%s|%s|%d|%s", orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment));

            custkey = shippriority = null;
            orderstatus = orderpriority = clerk = comment = null;
            totalprice = null;
            orderdate = null;
        }

        // Close the batch scanner
        dataScanner.close();

        long finish = System.currentTimeMillis();

        System.out.format("Found %d orders in %s ms\n", numTweets,
                (finish - start));
        return 0;
    }

    private void validateExists(Connector conn, String table)
    {
        if (!conn.tableOperations().exists(table)) {
            throw new InvalidParameterException(format("Error: Table %s does not exist",
                    DATA_TABLE));
        }
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

    @Override
    public Options getOptions()
    {
        Options opts = new Options();
        opts.addOption(OptionBuilder.withLongOpt("clerk-id").withDescription("List of Clerk IDs to search for orders")
                .hasArgs().isRequired().create(CLERK_ID));
        return opts;
    }
}