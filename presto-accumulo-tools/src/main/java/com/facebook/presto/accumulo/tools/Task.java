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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

/**
 * Extend this class to abstract away the command-line execution of the task. These classes are
 * auto-discovered by {@link Main} and added to list of tools. Tasks should be implemented in a way
 * to enable command-line execution of the tasks.
 */
public abstract class Task
{
    /**
     * Gets the name of the task/tool to be displayed to the user and used to identify via the
     * command line
     *
     * @return Task name
     */
    public abstract String getTaskName();

    /**
     * Gets the description of the task/tool to be displayed to the user
     *
     * @return Task description
     */
    public abstract String getDescription();

    /**
     * Gets the command line Options of the task/tool
     *
     * @return Options
     */
    public abstract Options getOptions();

    /**
     * Run the task with the given AccumuloConfig and parsed CommandLine
     *
     * @param config
     *            Accumulo config
     * @param cmd
     *            Parsed command line
     * @return 0 if successful, non-zero otherwise
     * @throws Exception
     */
    public abstract int run(AccumuloConfig config, CommandLine cmd)
            throws Exception;

    /**
     * Protected method to log an error if the given object is null, and returning 1 if it is null
     *
     * @param o
     *            Object to test
     * @param name
     *            Name of the parameter (for logging)
     * @return Zero if not null, one if null
     */
    protected int checkParam(Object o, String name)
    {
        if (o == null) {
            System.err.println(format("Parameter %s is not set", name));
            return 1;
        }
        return 0;
    }
}
