package bloomberg.presto.accumulo.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class TpchDBGenInvoker {
    private static final Logger LOG = Logger.getLogger(TpchDBGenInvoker.class);

    public static void run(File dbgendir, float scale) throws Exception {

        ProcessBuilder bldr = new ProcessBuilder("./dbgen", "-vf", "-s",
                Float.toString(scale));
        bldr.directory(dbgendir);
        bldr.redirectErrorStream(true);

        final Process p = bldr.start();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {

                BufferedReader rdr = new BufferedReader(
                        new InputStreamReader(p.getInputStream()));
                String line;
                try {
                    while ((line = rdr.readLine()) != null) {
                        LOG.info(line);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();
        int c = p.waitFor();
        if (c == 0) {
            LOG.info("Finished data generation");
        } else {
            throw new RuntimeException(
                    "Process exited with non-zero return code: " + c);
        }
    }
}
