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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.scenario.internal.AbstractProcessingTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.model.processing.AcquiringTx;
import org.apache.ignite.scenario.internal.model.processing.AcquiringTxKey;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Select a random card and get total amount of money transferred to a card within some time period.
 * Raise the reconciliation flag for processed records.
 */
public class AcquiringReconciliationTask extends AbstractProcessingTask {
    private static final Logger LOG = LogManager.getLogger(AcquiringReconciliationTask.class.getName());

    private static final int MAX_RECORDS_TO_PROCESS = 1000;

    private int execThreads = Runtime.getRuntime().availableProcessors() / 2;

    private long timeThresholdMillis;

    public AcquiringReconciliationTask(PocTesterArguments args) {
        super(args);
    }

    public AcquiringReconciliationTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    @Override
    protected String reportString() throws Exception {
        return null;
    }

    @Override
    public void init() throws IOException {
        super.init();

        timeThresholdMillis = props.getLong("timeThreshold", DFLT_CLEANUP_THRESHOLD_SECONDS) * 1000;
    }

    @Override
    protected void body0() throws Exception {
        LOG.info("Iteration start");

        long timeNow = Instant.now().toEpochMilli();
        int cardsProcessed = 0;

        ClusterGroup clusterGroup = ignite().cluster().forDataNodes(ACQUIRING_TX_CACHE_NAME);
        IgniteCompute compute = ignite().compute(clusterGroup);

        // Create an executor service
        ExecutorService execSrv = Executors.newFixedThreadPool(execThreads);
        List<Future<?>> futList = new ArrayList<>();

        // Submit compute runnables to the executor service
        List<Long> cardsNumFromTx = getCardsNumFromTx(MAX_RECORDS_TO_PROCESS, false, false);

        LOG.info("Unique cards to process: {}", cardsNumFromTx.size());

        for (long targetCardNum : cardsNumFromTx) {
            Future fut = execSrv.submit(() -> {
                Collection<Long> totals = compute.broadcast(
                        new ReconciliationTask(timeNow - timeThresholdMillis, targetCardNum)
                );

                long totalAmount = 0;

                for (Long total : totals)
                    totalAmount += total;

                LOG.info("Total amount deposited to card {}: {}", targetCardNum, totalAmount);
            });

            futList.add(fut);
        }

        for (Future<?> fut : futList) {
            fut.get();
            cardsProcessed++;
        }

        if (cardsProcessed == 0)
            LOG.warn("No records found yet.");
        else
            LOG.info("Total cards processed: {}", cardsProcessed);

        execSrv.shutdown();

        LOG.info("Iteration end");

        PocTesterUtils.sleep(interval);
    }

    /**
     * Select unprocessed acquiring records for a card.
     */
    private static class ReconciliationTask implements IgniteCallable<Long> {
        private long time;

        private long targetCardNum;

        private static final String SQL_SELECT_TX = "SELECT amount FROM " + ACQUIRING_TX_TABLE_NAME +
                " WHERE (reconciled = false AND replicated = false AND ts < ? AND tgtPan = ?)";

        private static final String SQL_UPDATE_TX_RECONC_FLAG = "UPDATE " + ACQUIRING_TX_TABLE_NAME +
                " SET reconciled = true" +
                " WHERE (reconciled = false AND replicated = false AND ts < ? AND tgtPan = ?)";

        @IgniteInstanceResource
        Ignite ignite;

        ReconciliationTask(long time, long targetCardNum) {
            this.time = time;
            this.targetCardNum = targetCardNum;
        }

        @Override
        public Long call() {
            Long res = 0L;

            IgniteCache<AcquiringTxKey, AcquiringTx> txCache = ignite.cache(ACQUIRING_TX_CACHE_NAME);

            SqlFieldsQuery txSelectQry = new SqlFieldsQuery(SQL_SELECT_TX)
                    .setArgs(time, targetCardNum)
                    .setLocal(true);

            for (List<?> row : txCache.query(txSelectQry).getAll()) {
                res += (Long) row.get(0);
            }

            // Set reconciliation flag to 'true'
            SqlFieldsQuery txUpdate = new SqlFieldsQuery(SQL_UPDATE_TX_RECONC_FLAG)
                    .setArgs(time, targetCardNum)
                    .setLocal(true);

            txCache.query(txUpdate);

            return res;
        }
    }
}
