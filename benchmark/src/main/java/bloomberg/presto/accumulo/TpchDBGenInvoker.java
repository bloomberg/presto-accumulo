package bloomberg.presto.accumulo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.InvalidParameterException;

import io.airlift.log.Logger;

public class TpchDBGenInvoker {

    public static void run(File dbgendir, String scale) throws Exception {

        if (!dbgendir.exists() || dbgendir.isFile()) {
            throw new InvalidParameterException(
                    dbgendir + " does not exist or is not a directory");
        }

        ProcessBuilder bldr = new ProcessBuilder("dbgen", "-vf", "-s", scale);
        bldr.directory(dbgendir);

        final Process p = bldr.start();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {

                BufferedReader rdr = new BufferedReader(
                        new InputStreamReader(p.getInputStream()));
                String line;
                try {
                    while ((line = rdr.readLine()) != null) {
                        Logger.get(getClass()).info(line);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();
        int c = p.waitFor();
        if (c != 0) {
            throw new RuntimeException(
                    "Process exited with non-zero return code: " + c);
        }
    }
}
