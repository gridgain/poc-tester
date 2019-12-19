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
import org.apache.ignite.scenario.internal.model.processing.Account;
import org.apache.ignite.scenario.internal.model.processing.AccountKey;
import org.apache.ignite.scenario.internal.model.processing.AcquiringTx;
import org.apache.ignite.scenario.internal.model.processing.AcquiringTxKey;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.ignite.transactions.Transaction;
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
 * Get acquiring records which are reconciled but not replicated yet.
 * Transfer money between accounts mentioned in these records.
 * Set the replication flag to 'true'.
 */
public class AcquiringSettlementTask extends AbstractProcessingTask {
    private static final Logger LOG = LogManager.getLogger(AcquiringSettlementTask.class.getName());

    private static final int MAX_RECORDS_TO_PROCESS = 1000;

    private int execThreads = Runtime.getRuntime().availableProcessors() / 2;

    private long timeThresholdMillis;

    public AcquiringSettlementTask(PocTesterArguments args) {
        super(args);
    }

    public AcquiringSettlementTask(PocTesterArguments args, TaskProperties props) {
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
        List<Long> cardsNumFromTx = getCardsNumFromTx(MAX_RECORDS_TO_PROCESS, true, false);

        LOG.info("Unique cards to process: {}", cardsNumFromTx.size());

        for (long cardNum : cardsNumFromTx) {
            Future fut = execSrv.submit(() -> {
                Collection<Integer> counters = compute.broadcast(
                        new SettlementTask(timeNow - timeThresholdMillis, cardNum)
                );

                int txProcessed = 0;

                for (Integer counter : counters)
                    txProcessed += counter;

                LOG.info("Transfers made for card {}: {}", cardNum, txProcessed);
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
     * Select
     */
    private static class SettlementTask implements IgniteCallable<Integer> {
        private long time;

        private long cardNum;

        private static final String SQL_SELECT_TX = "SELECT tgtPan, srcPan, tgtAcct, srcAcct, amount FROM " + ACQUIRING_TX_TABLE_NAME +
                " WHERE (reconciled = true AND replicated = false AND ts < ? AND tgtPan = ?)";

        private static final String SQL_UPDATE_REPL_FLAG = "UPDATE " + ACQUIRING_TX_TABLE_NAME +
                " SET replicated = true" +
                " WHERE (reconciled = true AND replicated = false AND ts < ? AND tgtPan = ?)";

        public SettlementTask(long time, long cardNum) {
            this.time = time;
            this.cardNum = cardNum;
        }

        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public Integer call() throws Exception {
            Integer cnt = 0;

            IgniteCache<AcquiringTxKey, AcquiringTx> txCache = ignite.cache(ACQUIRING_TX_CACHE_NAME);
            IgniteCache<AccountKey, Account> accCache = ignite.cache(ACCOUNTS_CACHE_NAME);

            SqlFieldsQuery txSelectQry = new SqlFieldsQuery(SQL_SELECT_TX)
                    .setArgs(time, cardNum)
                    .setLocal(true);

            for (List<?> row : txCache.query(txSelectQry).getAll()) {
                long tgtPan = (Long) row.get(0);
                long srcPan = (Long) row.get(1);
                long tgtAcct = (Long) row.get(2);
                long srcAcct = (Long) row.get(3);
                long amount = (Long) row.get(4);

                AccountKey k0 = new AccountKey(srcAcct, srcPan);
                AccountKey k1 = new AccountKey(tgtAcct, tgtPan);

                // Transfer money between accounts
                try (Transaction tx = ignite.transactions().txStart()) {
                    Account acc0 = accCache.get(k0);
                    Account acc1 = accCache.get(k1);

                    acc0.credit(amount);
                    acc1.debit(amount);

                    accCache.put(k0, acc0);
                    accCache.put(k1, acc1);

                    tx.commit();

                    cnt++;
                }
                catch (Exception e) {
                    LOG.error("Failed to transfer money from {} to {}", srcAcct, tgtAcct);
                }
            }

            // Set 'replicated = true' for all fetched records
            SqlFieldsQuery txUpdate = new SqlFieldsQuery(SQL_UPDATE_REPL_FLAG)
                    .setArgs(time, cardNum)
                    .setLocal(true);

            txCache.query(txUpdate);

            return cnt;
        }
    }
}
