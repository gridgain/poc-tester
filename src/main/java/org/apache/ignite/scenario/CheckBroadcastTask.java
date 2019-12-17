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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.scenario.internal.AbstractReportingTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.model.SampleObject;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Fill cache with generated data: - key is random UUID. - value is generated BinaryObject with random String fields.
 * Parameters: size fields fieldSize
 */
public class CheckBroadcastTask extends AbstractReportingTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(CheckBroadcastTask.class.getName());

    /** */
    private long lastTotalSum;

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public CheckBroadcastTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {

        long currSum = 0;

        long startTime = System.currentTimeMillis();

        LOG.info("Making stop flag.");

        incMsgFlag("pauseTX");

        incMsgFlag("pauseRestart");

        waitForTx();

        if (!waitIfStopFlag("pauseCheck"))
            return;

        waitForAffinity();

        LOG.info("Starting to count sum.");

        boolean done = false;

        while (!done) {

            currSum = 0;

            try {
                for (String cacheName : getCacheNameList()) {

                    final Collection<Long> resList = ignite().compute(ignite().cluster().
                        forDataNodes(cacheName)).broadcast(new Checker(ignite(), cacheName));

                    for (Long res : resList) {
                        if (res == null)
                            LOG.error("One of the results from remote nodes is null");
                        else
                            currSum = currSum + res;
                    }
                }

                done = true;
            }
            catch (Exception e) {
                LOG.error(e.getMessage());

                sleep(5000L);
            }

            long currTime = System.currentTimeMillis();

            if (currTime > startTime + timeToWork * 1000L) {
                LOG.info(String.format("Time to work (%d seconds) is exceeded. Will not try to count sum any longer.",
                    timeToWork));

                LOG.info("Removing stop flag.");

                decMsgFlag("pauseTX");

                decMsgFlag("pauseRestart");

                return;
            }
        }

        if (lastTotalSum != currSum)
            LOG.warn(String.format("Current sum %d is not equal last sum %d", currSum, lastTotalSum));
        else
            LOG.info(String.format("Current sum %d equals last sum %d", currSum, lastTotalSum));

        lastTotalSum = currSum;

        LOG.info("Removing stop flag.");

        decMsgFlag("pauseTX");

        decMsgFlag("pauseRestart");

        sleep(interval);

    }

    /** {@inheritDoc} */
    @Override public String reportString() {
        return String.valueOf(lastTotalSum);
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        super.tearDown();
    }

    /** {@inheritDoc} */
    protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "number");
        hdrMap.put("data", "total_sum");
    }

    private class Checker implements IgniteCallable<Long> {

        private String cacheName;

        public Checker(Ignite ignite, String cacheName) {
            this.cacheName = cacheName;
            this.ignite = ignite;
        }

        /** Ignite instance. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** {@inheritDoc} */
        @Override public Long call() throws Exception {
            Long res = 0L;

            final Affinity aff = ignite.affinity(cacheName);

            final int[] partIds = aff.primaryPartitions(((IgniteEx)ignite).localNode());

            final ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new IgniteThreadFactory(ignite.name(), "checksum-worker"));

            final List<Callable<Long>> callList = new ArrayList<>(partIds.length);

            for (final int partId : partIds) {
                Callable<Long> call = new Callable<Long>() {
                    @Override public Long call() {
                        long partSum = 0L;

                        try (final QueryCursor entries = ignite.cache(cacheName).query(new ScanQuery<>(partId))) {
//                            for (Object e : entries) {
//                                Cache.Entry entry = (Cache.Entry)e;
//
//                                SampleObject val = (SampleObject)entry.getValue();
//
//                                partSum = partSum + val.getBalance();
//                            }

                            Iterator<Cache.Entry> iter = entries.iterator();

                            while (iter.hasNext()) {
                                Cache.Entry entry = iter.next();

                                SampleObject val = (SampleObject)entry.getValue();

                                partSum = partSum + val.getBalance();
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();

                            LOG.error(e.getMessage());

                            return null;
                        }

                        return partSum;
                    }
                };

                callList.add(call);
            }

            final List<Future<Long>> futList = exec.invokeAll(callList);

            for (Future<Long> future : futList) {
                if (future.get() == null)
                    return null;

                res = res + future.get();
            }

            return res;
        }

    }
}
