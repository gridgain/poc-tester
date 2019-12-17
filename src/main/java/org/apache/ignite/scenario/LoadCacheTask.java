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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.scenario.internal.AbstractLoadTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.hms;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Pre-load data into caches with IgniteDataStreamer.
 * <p>
 * - Generates {@link SampleObject} for a given number of caches containing a given number of keys in each cache.
 * - Loads generated data to server nodes.
 */
public class LoadCacheTask extends AbstractLoadTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(LoadCacheTask.class.getName());

    /** */
    private AtomicInteger cacheCnt = new AtomicInteger();

    private boolean streamerAllowOverwrite;

    /** */
    public LoadCacheTask(PocTesterArguments args,
        TaskProperties props) {
        super(args, props);

        streamerAllowOverwrite = props.getBoolean("allowOverwrite", Boolean.FALSE);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        assert fieldCnt > 0;

        Ignite ignite = ignite();

        ExecutorService execServ = Executors.newFixedThreadPool(loadThreads);

        List<Future<?>> futList = new ArrayList<>();

        LOG.info("Caches count: {}", ignite.cacheNames().size());
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

    /**
     * Loader implementation.
     */
    private class Loader implements Runnable {
        /** */
        private final String cacheName0;

        /** */
        private int maxTryCnt = 3;

        /** */
        private long totalKeys;

        /** */
        Loader(String cacheName0) {
            this.cacheName0 = cacheName0;
            this.totalKeys = taskLoadTo - taskLoadFrom + 1;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            boolean keysLoaded = false;
            for (int tryCnt = 1; tryCnt <= maxTryCnt; tryCnt++ ) {
                try (IgniteDataStreamer<Object, Object> streamer = ignite().dataStreamer(cacheName0)) {
                    streamer.allowOverwrite(streamerAllowOverwrite);

                    LOG.info(String.format("Starting to load data into cache %s. Try %d out of %d.",
                            cacheName0, tryCnt, maxTryCnt));
                    for (long key = taskLoadFrom; key <= taskLoadTo; key++) {
                        streamer.addData(key, new SampleObject(key, fieldCnt, fieldLen));

                        if (key % 1000 == 0 && key > taskLoadFrom) {
                            String time = hms();

                            long pcnt = ((key - taskLoadFrom) * 100) / totalKeys;
                            String pctStr = pcnt + "%";
                            String rep = String.format("[%s] %d keys (%s) has been added to data streamer into cache %s.",
                                    time, key, pctStr, cacheName0);
                            LOG.info(rep);

                            if (key % 10000 == 0)
                                System.out.println(rep);
                        }
                    }
                } catch (Exception e) {
                    LOG.error(String.format("Failed to load data into cache %s.", cacheName0), e);
                    throw e;
                }

                // Check that some last keys was loaded successfully
                if (check()) {
                    keysLoaded = true;
                    break;
                } else {
                    LOG.info(String.format("Some data is missing in cache %s. Will try to load data again.",
                            cacheName0));
                }
            }

            if (keysLoaded) {
                LOG.info(String.format("Data streamer has finished loading data into cache %s.", cacheName0));
                LOG.info(String.format("Data streamer has loaded data into %d caches.", cacheCnt.incrementAndGet()));
            } else {
                LOG.error(String.format("Data streamer failed to load all the data into cache %s.", cacheName0));
                // TODO: set dataLost flag
                if (cpOnFailedLoad) {
                    LOG.info("Task will copy work directories.");
                    copyWorkDirs();
                }
                // TODO: what is this?
                keepLock.getAndSet(true);
            }
        }

        public boolean check() {
            long shift = totalKeys / 10;
            int max_retries = 3;
            int checkInterval = 5000;
            long missedKeys = 0L;

            for (int try_cnt = 1; try_cnt <= max_retries; try_cnt++) {
                LOG.info(String.format("Checking last %d loaded keys in cache %s. Try %d out of %d",
                        shift,
                        cacheName0,
                        try_cnt,
                        max_retries)
                );

                missedKeys = 0;
                for (long i = taskLoadTo - shift; i <= taskLoadTo; i++) {
                    if (ignite().cache(cacheName0).get(i) == null) {
                        missedKeys++;
                        String consId = null;

                        try {
                            final Affinity af = ignite().affinity(cacheName0);
                            consId = af.mapKeyToNode(i).consistentId().toString();
                        }
                        catch (Exception e){
                            LOG.error("Failed to get node consistent id.", e);
                        }

                        LOG.error(String.format("Failed to find value for key %d in cache %s. " +
                                "Node id mapped for key = %s", i, cacheName0, consId));
                    }
                }

                if (missedKeys == 0) {
                    break;
                } else {
                    LOG.error(String.format("Failed to find values for at least %d keys in the cache %s", missedKeys,
                            cacheName0));
                    LOG.error(String.format("Waiting %d seconds before performing next check for cache %s",
                            checkInterval / 1000L, cacheName0));
                    sleep(checkInterval);
                }
            }

            return (missedKeys == 0);
        }
    }
}
