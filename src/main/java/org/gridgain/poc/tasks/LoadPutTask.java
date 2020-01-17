/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.poc.tasks;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.gridgain.poc.framework.worker.task.AbstractLoadTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.gridgain.poc.framework.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Fill cache with generated data: - key is random UUID. - value is generated BinaryObject with random String fields.
 * Parameters: size fields fieldSize
 */
public class LoadPutTask extends AbstractLoadTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(LoadPutTask.class.getName());

    public LoadPutTask(PocTesterArguments args,
        TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        assert fieldCnt > 0;

        Ignite ignite = ignite();

        ExecutorService execServ = Executors.newFixedThreadPool(loadThreads);

        List<Future<?>> futList = new ArrayList<>();

        for (String cacheName : getCacheNameList())
            futList.add(execServ.submit(new LoadPutTask.Loader(cacheName)));

        for (Future f : futList) {
            try {
                f.get();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        execServ.shutdown();
    }

    private class Loader implements Runnable {
        private String cacheName0;

        public Loader(String cacheName0) {
            this.cacheName0 = cacheName0;

        }

        /** {@inheritDoc} */
        @Override public void run() {
            long total = taskLoadTo - taskLoadFrom;

            for (long i = taskLoadFrom; i < taskLoadTo; i++) {
                long curTime = System.currentTimeMillis();

                ignite().cache(cacheName0).put(i, new SampleObject(i, fieldCnt, fieldLen));
                if (i % 10000 == 0 && i > 0) {
                    DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
                    String time = timeFormat.format(new Date(curTime));

                    long pct = ((i - taskLoadFrom) * 100) / total;

                    String pctStr = pct + "%";

                    String rep = String.format("[%s] %d keys (%s) has been added to cache %s ",
                        time, i, pctStr, cacheName0);

                    LOG.info(rep);

                    if (i % 100000 == 0)
                        System.out.println(rep);
                }
            }
            ignite().cache(cacheName0).put((taskLoadTo), new SampleObject(taskLoadTo, fieldCnt, fieldLen));
        }
    }

}
