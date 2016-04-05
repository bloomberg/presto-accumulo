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
package com.facebook.presto.accumulo.benchmark;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class TpchDBGenInvoker
{
    private static final Logger LOG = Logger.getLogger(TpchDBGenInvoker.class);

    public static void run(File dbgendir, float scale)
            throws Exception
    {

        ProcessBuilder bldr = new ProcessBuilder("./dbgen", "-vf", "-s", Float.toString(scale));
        bldr.directory(dbgendir);
        bldr.redirectErrorStream(true);

        final Process p = bldr.start();

        Thread t = new Thread(new Runnable()
        {
            @Override
            public void run()
            {

                BufferedReader rdr = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                try {
                    while ((line = rdr.readLine()) != null) {
                        LOG.info(line);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();
        int c = p.waitFor();
        if (c == 0) {
            LOG.info("Finished data generation");
        }
        else {
            throw new RuntimeException("Process exited with non-zero return code: " + c);
        }
    }
}
