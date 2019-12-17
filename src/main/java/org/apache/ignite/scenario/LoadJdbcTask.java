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

package org.apache.ignite.scenario;

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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.scenario.internal.AbstractLoadTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.model.Organisation;
import org.apache.ignite.scenario.internal.model.Person;
import org.apache.ignite.scenario.internal.model.PersonKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Fill cache with generated data: - key is random UUID. - value is generated BinaryObject with random String fields.
 * Parameters: size fields fieldSize
 */
public class LoadJdbcTask extends AbstractLoadTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(LoadJdbcTask.class.getName());

    public LoadJdbcTask(PocTesterArguments args,
        TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        Ignite ignite = ignite();

        ExecutorService execServ = Executors.newFixedThreadPool(loadThreads);

        List<Future<?>> futList = new ArrayList<>();

        for (String cacheName : ignite.cacheNames()) {
            if (!checkNameRange(cacheName))
                continue;

            futList.add(execServ.submit(new Loader(cacheName)));
        }

        for (Future f : futList) {
            try {
                f.get();
            }
            catch (InterruptedException | ExecutionException e) {
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
            try (IgniteDataStreamer<Object, Object> streamer = ignite().dataStreamer(cacheName0)) {
                long total = (taskLoadTo - taskLoadFrom) * (taskLoadTo - taskLoadFrom);

                for (long i = taskLoadFrom; i <= taskLoadTo; i++) {
                    Organisation org = new Organisation(i, "org" + i);

                    streamer.addData(i, org);

                    for (long j = taskLoadFrom; j <= taskLoadTo; j++) {

                        long curTime = System.currentTimeMillis();

                        PersonKey orgPersonKey = new PersonKey(i, j);
                        Person person = new Person("pName" + i, "pLastName" +i);

                        streamer.addData(orgPersonKey, person);
                        if ((i * j) % 10000 == 0 && j > taskLoadFrom) {
                            DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
                            String time = timeFormat.format(new Date(curTime));

                            long pct = ((j * i) * 100) / total;

                            String pctStr = pct + "%";

                            String rep = String.format("[%s] %d keys (%s) has been added to data streamer to cache %s ",
                                time, j * i, pctStr, cacheName0);

                            LOG.info(rep);

                            if (j % 100000 == 0)
                                System.out.println(rep);
                        }
                    }
                }

                streamer.close();

//                long shift = total > 1000 ? 1000 : total;
//
//                for (long i = taskLoadTo - 1; i > taskLoadTo - shift; i--) {
//                    if (ignite().cache(cacheName0).get(i) == null) {
//                        System.out.println("Failed to find value for key " + i + " in cache " + cacheName0);
//                        LOG.error("Failed to find value for key " + i + " in cache " + cacheName0);
//                    }
//                }
//
//                ignite().cache(cacheName0).put((taskLoadTo), new SampleObject(fieldCount, fieldLength));
            }
            catch (Exception e) {
                LOG.error("Failed to load data", e.getMessage());
                e.printStackTrace();
            }
        }
    }

}
