package bloomberg.presto.accumulo.benchmark;

import java.io.File;

public class QueryFormatter
{

    public static void run(String schema, File benchmarkDir)
            throws Exception
    {
        ProcessBuilder bldr = new ProcessBuilder("sh", "set_vars.sh", schema);
        bldr.directory(benchmarkDir);
        bldr.start().waitFor();
    }
}
