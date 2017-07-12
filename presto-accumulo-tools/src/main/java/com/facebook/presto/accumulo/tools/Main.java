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
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.accumulo.conf.AccumuloConfig.CARDINALITY_CACHE_EXPIRE_DURATION;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.CARDINALITY_CACHE_SIZE;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.INSTANCE;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.PASSWORD;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.USERNAME;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.ZOOKEEPERS;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.ZOOKEEPER_METADATA_ROOT;
import static java.lang.String.format;

/**
 * Main class for invoking tasks via the command line. Auto-detects implementations of {@link Task}
 * within the com.facebook.presto.accumulo.tools package and includes them in the list of available
 * tasks.
 */
@SuppressWarnings("static-access")
public class Main
        extends Configured
        implements Tool
{
    /**
     * List of all tasks
     */
    private static List<Task> tasks = ImmutableList.of(new PrestoBatchWriter());

    private static final Option HELP = OptionBuilder.withDescription("Print this help message").withLongOpt("help").create();
    private static final Option CONFIG = OptionBuilder.withDescription("accumulo.properties file").withLongOpt("config").hasArg().create('c');

    /**
     * Gets a {@link Task} that matches the given task name
     *
     * @param name Task name to find
     * @return The Task, or null if not found
     */
    public static Task getTask(String name)
    {
        for (Task t : tasks) {
            if (t.getTaskName().equals(name)) {
                return t;
            }
        }

        return null;
    }

    public static List<Task> getTasks()
    {
        return tasks;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int run(String[] args)
            throws Exception
    {
        // If no arguments, print help
        if (args.length == 0) {
            printTools();
            return 1;
        }

        // Get the tool name from the first argument
        String toolName = args[0];
        Task t = Main.getTask(toolName);
        if (t == null) {
            System.err.println(format("Unknown tool %s", toolName));
            printTools();
            return 1;
        }

        // Add the help option and all options for the tool
        Options opts = new Options();
        opts.addOption(HELP);
        opts.addOption(CONFIG);
        for (Option o : (Collection<Option>) t.getOptions().getOptions()) {
            opts.addOption(o);
        }

        // Parse command lines
        CommandLine cmd;
        try {
            cmd = new GnuParser().parse(opts, Arrays.copyOfRange(args, 1, args.length));
        }
        catch (ParseException e) {
            System.err.println(e.getMessage());
            printHelp(t);
            return 1;
        }

        // Print help if the option is set
        if (cmd.hasOption(HELP.getLongOpt())) {
            printHelp(t);
            return 0;
        }
        else {
            AccumuloConfig config;
            if (cmd.hasOption(CONFIG.getLongOpt())) {
                config = fromFile(new File(cmd.getOptionValue(CONFIG.getLongOpt())));
            }
            else {
                // Validate PRESTO_HOME is set to pull accumulo properties from config path
                String prestoHome = System.getenv("PRESTO_HOME");
                if (prestoHome == null) {
                    System.err.println("PRESTO_HOME is not set.  This is required to locate the etc/catalog/accumulo.properties file, or use --config option");
                    System.exit(1);
                }

                // Create an AccumuloConfig from the accumulo properties file
                config = fromFile(new File(System.getenv("PRESTO_HOME"), "etc/catalog/accumulo.properties"));
            }

            // Run the tool and print help if anything bad happens
            int code = t.run(config, cmd);
            if (code != 0) {
                printHelp(t);
            }
            return code;
        }
    }

    /**
     * Prints all tools and brief descriptions.
     */
    private void printTools()
    {
        System.out.println("Usage: java -jar <jarfile> <tool> [args]");
        System.out.println("Execute java -jar <jarfile> <tool> --help to see help for a tool.\nAvailable tools:");
        for (Task t : getTasks()) {
            System.out.println("\t" + t.getTaskName() + "\t" + t.getDescription());
        }
    }

    /**
     * Prints the help for a given {@link Task}
     *
     * @param t Task to print help
     */
    @SuppressWarnings("unchecked")
    private void printHelp(Task t)
    {
        Options opts = new Options();
        opts.addOption(HELP);
        opts.addOption(CONFIG);
        for (Option o : (Collection<Option>) t.getOptions().getOptions()) {
            opts.addOption(o);
        }

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(format("usage: java -jar <jarfile> %s [args]", t.getTaskName()), opts);
    }

    public static AccumuloConfig fromFile(File f)
            throws ConfigurationException
    {
        if (!f.exists() || f.isDirectory()) {
            throw new ConfigurationException(format("File %s does not exist or is a directory", f));
        }
        PropertiesConfiguration props = new PropertiesConfiguration(f);
        props.setThrowExceptionOnMissing(true);

        AccumuloConfig config = new AccumuloConfig();
        config.setCardinalityCacheExpiration(Duration.valueOf(props.getString(CARDINALITY_CACHE_EXPIRE_DURATION, "5m")));
        config.setCardinalityCacheSize(props.getInt(CARDINALITY_CACHE_SIZE, 100_000));
        config.setInstance(props.getString(INSTANCE));
        config.setPassword(props.getString(PASSWORD));
        config.setUsername(props.getString(USERNAME));
        config.setZkMetadataRoot(props.getString(ZOOKEEPER_METADATA_ROOT, "/presto-accumulo"));
        config.setZooKeepers(props.getString(ZOOKEEPERS));
        return config;
    }

    public static void main(String[] args)
            throws Exception
    {
        ToolRunner.run(new Configuration(), new Main(), args);
    }
}
