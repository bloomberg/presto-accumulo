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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.reflections.Reflections;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.lang.String.format;

public class Main
        extends Configured
        implements Tool
{

    private static List<Task> tasks = new ArrayList<>();

    static {
        try {
            Reflections reflections = new Reflections("bloomberg.presto.accumulo.tools");
            for (Class<? extends Task> task : reflections.getSubTypesOf(Task.class)) {
                tasks.add(task.newInstance());
            }
        }
        catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

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
        // Sort by name
        tasks.sort(new Comparator<Task>()
        {
            @Override
            public int compare(Task o1, Task o2)
            {
                return o1.getTaskName().compareTo(o2.getTaskName());
            }
        });

        return tasks;
    }

    @Override
    public int run(String[] args)
            throws Exception
    {
        if (args.length < 1) {
            printHelp();
            return 0;
        }

        String prestoHome = System.getenv("PRESTO_HOME");
        AccumuloConfig config =
                AccumuloConfig.from(new File(prestoHome, "etc/catalog/accumulo.properties"));

        String toolName = args[0];
        Task t = Main.getTask(toolName);
        if (t == null) {
            System.err.println(format("Unknown tool %s", toolName));
            printHelp();
            return 1;
        }

        if (t.isNumArgsOk(args.length - 1)) {
            int code = t.run(config, Arrays.copyOfRange(args, 1, args.length));
            if (code != 0) {
                printHelp();
            }
            return code;
        }
        else {
            System.err.println("Invalid number of parameters");
            printHelp();
            return 1;
        }
    }

    private void printHelp()
    {
        System.out.println("Usage: java -jar <jarfile> <tool> [args]");
        System.out.println("Available tools:");
        for (Task t : getTasks()) {
            System.out.println(t.getHelp());
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        String prestoHome = System.getenv("PRESTO_HOME");
        if (prestoHome == null) {
            System.err.println("PRESTO_HOME is not set");
            System.exit(1);
        }
        ToolRunner.run(new Configuration(), new Main(), args);
    }
}
