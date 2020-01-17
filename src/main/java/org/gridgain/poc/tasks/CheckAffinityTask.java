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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.gridgain.poc.framework.worker.task.AbstractReportingTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.gridgain.poc.framework.model.SampleObject;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.sleep;

/**
 * This task should be started along with {@link TxBalanceTask}.
 *
 * The task does the following:
 * - Wait for all running transactions to end.
 * - Run idle_verify to check whether differences exist between primary and backup partitions.
 *   If differences are found, re-check several times.
 * - If there are no differences, calculate sums of all balances in {@link SampleObject}.
 * - If last calculated sum does not equal the very first sum, then stop.
 */
public class CheckAffinityTask extends AbstractReportingTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(CheckAffinityTask.class.getName());

    /** */
    private long lastTotalSum = 0;

    /** */
    private long startTime;

    /**
     * @param args
     * @param props
     */
    public CheckAffinityTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        startTime = System.currentTimeMillis();

        for (String cacheName : getCacheNameList()) {
            long size = ignite().cache(cacheName).size();

            LOG.info(String.format("Cache name = %s; Size = %d", cacheName, size));
        }
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        if (stopOnStopFlags()) {
            LOG.warn("Found raised stop flag(s). The task will stop.");
            tearDown();
        }

        if (!checkWaitFlags())
            tearDown();

        LOG.info("Iteration start");

        raiseSyncFlags();

        waitForAffinity();

        // Wait for all running transactions to end before calling Verify
        waitForTx();

        try {
            verifyBackupPartitions(new HashSet<>(getCacheNameList()));
        }
        catch (Exception e) {
            LOG.error("Failed to verify backup partitions.", e);
        }

        long currSum = countSum();

        if (lastTotalSum == 0)
            LOG.info("Initial sum is {}", currSum);
        else if (lastTotalSum != currSum) {
            LOG.error(String.format("Current sum %d is not equal last sum %d", currSum, lastTotalSum));

            LOG.info("Number of running transactions is {}", activeTx());

            for (String cacheName : getCacheNameList()) {
                long size = ignite().cache(cacheName).size();
                LOG.info(String.format("Cache name = %s; Size = %d", cacheName, size));
            }

            stopAndCollect();

            throw new TestFailedException();
        }
        else
            LOG.info(String.format("Current sum %d equals last sum %d", currSum, lastTotalSum));

        lastTotalSum = currSum;

        LOG.info("Removing stop flags for transactions and restart.");
        clearSyncFlags();

        LOG.info("Iteration end");

        sleep(interval);
    }

    /**
     * @return
     */
    private long countSum() {
        LOG.info("Starting to count sum.");

        long currSum = 0;

        boolean done = false;

        ExecutorService execServ = null;

        while (!done) {
            try {
                currSum = 0;

                execServ = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

                List<Future<Long>> futList = new ArrayList<>();

                for (String cacheName : getCacheNameList())
                    futList.add(execServ.submit(new Checker(ignite(), cacheName)));

                for (Future<Long> f : futList)
                    currSum += f.get(300_000L, TimeUnit.MILLISECONDS);

                done = true;
            }
            catch (Exception e) {
                LOG.error(e.getMessage(), e);

                sleep(5000L);
            }
            finally {
                if (execServ != null)
                    execServ.shutdown();
            }
        }

        return currSum;
    }

    /** {@inheritDoc} */
    @Override public String reportString() {
        return lastTotalSum == 0 ? null : String.valueOf(lastTotalSum);
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "number");
        hdrMap.put("data", "total_sum");
    }

    /**
     *
     */
    private class Checker implements IgniteCallable<Long> {
        /** */
        private String cacheName;

        /**
         * @param ignite
         * @param cacheName
         */
        Checker(Ignite ignite, String cacheName) {
            this.cacheName = cacheName;
            this.ignite = ignite;
        }

        /** Ignite instance. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** {@inheritDoc} */
        @Override public Long call() throws Exception {
            Long res = 0L;

            AffinityTopologyVersion waitTopVer = ((IgniteEx)ignite).context().discovery().topologyVersionEx();

            if (waitTopVer.topologyVersion() <= 0)
                waitTopVer = new AffinityTopologyVersion(1, 0);

            IgniteInternalFuture<?> exchFut =
                ((IgniteEx)ignite).context().cache().context().exchange().affinityReadyFuture(waitTopVer);

            if (exchFut != null && !exchFut.isDone()) {
                try {
                    exchFut.get(5000L);
                }
                catch (IgniteCheckedException e) {
                    LOG.error("Failed to wait for exchange [topVer=" + waitTopVer +
                        ", node=" + ((IgniteKernal)ignite).name() + ']', e);
                }
            }

            final Affinity af = ignite.affinity(cacheName);

            final int partitions = af.partitions();

            final ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new IgniteThreadFactory(ignite.name(), "checksum-worker"));

            final Collection<Callable<Long>> callList = new ArrayList<>(partitions);

            for (int i = 0; i < partitions; i++) {
                final int part = i;

                Callable<Long> call = new Callable<Long>() {
                    @Override public Long call() {
                        final IgniteCallable<Long> task = new AffinityCheckClosure(cacheName, part);

                        return ignite.compute().affinityCall(cacheName, part, task);
                    }
                };

                callList.add(call);
            }

            final List<Future<Long>> futList = exec.invokeAll(callList);

            for (Future<Long> future : futList)
                res += future.get();

            exec.shutdown();

            return res;
        }
    }

    /**
     * Calculates sum from partitions values.
     */
    private static class AffinityCheckClosure implements IgniteCallable<Long> {
        /** Name of cache for sum calculation. */
        private final String cacheName;

        /** Partition ID. */
        private final int partID;

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Constructor.
         *
         * @param cacheName Cache name.
         * @param partID Partition ID.
         */
        AffinityCheckClosure(String cacheName, int partID) {
            this.cacheName = cacheName;
            this.partID = partID;
        }

        /**
         * Count sum of fields in partition.
         *
         * @return Long.
         */
        @Override public Long call() throws Exception {
            final QueryCursor entries = ignite.cache(cacheName).query(new ScanQuery<>(partID));

            return convertToLong(entries);
        }

        /**
         * Creates {@code BinaryObject} from cache entry list.
         *
         * @param entries {@code QueryCursor} cache entries.
         * @return {@code BinaryObject}
         */
        Long convertToLong(QueryCursor entries) {
            Iterator<Cache.Entry> iter = entries.iterator();

            long partSum = 0L;

            while (iter.hasNext()) {
                Cache.Entry entry = iter.next();

                SampleObject val = (SampleObject)entry.getValue();

                partSum += val.getBalance();
            }

            return partSum;
        }
    }
}
