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
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Date;

import static com.facebook.presto.accumulo.examples.Constants.CF;
import static com.facebook.presto.accumulo.examples.Constants.CLERK;
import static com.facebook.presto.accumulo.examples.Constants.COMMENT;
import static com.facebook.presto.accumulo.examples.Constants.CUSTKEY;
import static com.facebook.presto.accumulo.examples.Constants.DATA_TABLE;
import static com.facebook.presto.accumulo.examples.Constants.EMPTY_BYTES;
import static com.facebook.presto.accumulo.examples.Constants.INDEX_TABLE;
import static com.facebook.presto.accumulo.examples.Constants.ORDERDATE;
import static com.facebook.presto.accumulo.examples.Constants.ORDERPRIORITY;
import static com.facebook.presto.accumulo.examples.Constants.ORDERSTATUS;
import static com.facebook.presto.accumulo.examples.Constants.SHIPPRIORITY;
import static com.facebook.presto.accumulo.examples.Constants.TOTALPRICE;
import static com.facebook.presto.accumulo.examples.Constants.encode;
import static com.facebook.presto.accumulo.examples.Constants.sdformat;
import static java.lang.String.format;

@SuppressWarnings("static-access")
public class TpcHBatchWriter
        extends Task
{
    private static final String TASK_NAME = "tpchingest";
    private static final String DESCRIPTION = "Example for ingesting TPC-H 'orders' data set into Accumulo using a BatchWriter";

    private static final char ORDERS_OPT = 'f';

    @Override
    public int run(AccumuloConfig config, CommandLine cmd)
            throws Exception
    {
        Path orders = new Path(cmd.getOptionValue(ORDERS_OPT));
        final FileSystem fs = FileSystem.get(new Configuration());
        if (!fs.exists(orders)) {
            throw new FileNotFoundException(format("File %s does not exist or is a directory", orders));
        }

        ZooKeeperInstance inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
        Connector conn = inst.getConnector(config.getUsername(), new PasswordToken(config.getPassword()));

        validateTable(conn, DATA_TABLE);
        validateTable(conn, INDEX_TABLE);

        BatchWriterConfig bwc = new BatchWriterConfig();
        MultiTableBatchWriter mtbw = conn.createMultiTableBatchWriter(bwc);
        BatchWriter mainWrtr = mtbw.getBatchWriter(DATA_TABLE);
        BatchWriter indexWrtr = mtbw.getBatchWriter(INDEX_TABLE);

        long numTweets = 0, numIndex = 0;

        System.out.println(format("Reading from file: %s", orders));
        BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(orders)));

        // For each record in the file
        String line;
        while ((line = rdr.readLine()) != null) {
            // Split the line into fields
            String[] fields = line.split("\\|");
            if (fields.length < 9) {
                System.err.println(format("Record does not contain at least nine fields:\n%s", line));
                continue;
            }

            // Parse out the fields from strings
            Long orderkey = Long.parseLong(fields[0]);
            Long custkey = Long.parseLong(fields[1]);
            String orderstatus = fields[2];
            Double totalprice = Double.parseDouble(fields[3]);
            Date orderdate = sdformat.parse(fields[4]);
            String orderpriority = fields[5];
            String clerk = fields[6];
            Long shippriority = Long.parseLong(fields[7]);
            String comment = fields[8];

            // Create mutation for the row
            Mutation mutation = new Mutation(encode(orderkey));
            mutation.put(CF, CUSTKEY, encode(custkey));
            mutation.put(CF, ORDERSTATUS, encode(orderstatus));
            mutation.put(CF, TOTALPRICE, encode(totalprice));
            mutation.put(CF, ORDERDATE, encode(orderdate));
            mutation.put(CF, ORDERPRIORITY, encode(orderpriority));
            mutation.put(CF, CLERK, encode(clerk));
            mutation.put(CF, SHIPPRIORITY, encode(shippriority));
            mutation.put(CF, COMMENT, encode(comment));
            mainWrtr.addMutation(mutation);
            ++numTweets;

            // Create index mutation for the clerk
            Mutation idxClerk = new Mutation(encode(clerk));
            idxClerk.put(CF, encode(orderkey), EMPTY_BYTES);
            indexWrtr.addMutation(idxClerk);
            ++numIndex;
        }
        rdr.close();

        // Send the mutations to Accumulo and release resources
        mtbw.close();

        // Display how many tweets were inserted into Accumulo
        System.out.println(format("%d tweets Mutations inserted", numTweets));
        System.out.println(format("%d index Mutations inserted", numIndex));
        return 0;
    }

    private void validateTable(Connector conn, String table)
            throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException
    {
        // Create our table if it does not already exist
        if (conn.tableOperations().exists(table)) {
            // If it does, delete the table and create it again
            System.out.format("Deleting table %s\n", table);
            conn.tableOperations().delete(table);
        }

        System.out.format("Creating table %s\n", table);
        conn.tableOperations().create(table);
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
        opts.addOption(OptionBuilder.withLongOpt("path").withDescription("Path to the TPC-H orders.tbl file")
                .hasArg().isRequired().create(ORDERS_OPT));
        return opts;
    }
}