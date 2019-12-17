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

package org.apache.ignite.scenario.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.CacheException;

import org.apache.ignite.IgniteCacheRestartingException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Run transactions.
 */
public abstract class AbstractTxTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(AbstractTxTask.class.getName());

    protected static final String DFLT_TX_LEN_MODE = "defined";

    /** */
    private static final String DEFAULT_TX_CONCURRENCY = "PESSIMISTIC";

    /** */
    private static final String DEFAULT_TX_ISOLATION = "REPEATABLE_READ";

    /** */
    private static final long DFLT_TX_TIMEOUT = 300_000L;

    /** */
    protected static final int DEFAULT_COMMIT_PROBABILITY = 95;

    /** */
    protected int commitProb = DEFAULT_COMMIT_PROBABILITY;

    /** */
    protected int minTxLen = DFLT_MIN_TX_LEN;

    /** */
    private String txLenMode = DFLT_TX_LEN_MODE;

    /** */
    protected int putAllProb = 50;

    /** */
    private String probRange;

    /** */
    protected int[] probRangeArr;

    /** */
    private String putGetTxLenProbs;

    /** */
    protected int[] putGetProbArr;

    /** */
    private String putAllTxLenProbs;

    /** */
    protected int[] putAllProbArr;

    /** */
    private String removeTxProbs;

    /** */
    protected int[] removeTxProbArr;

    /** */
    private TransactionConcurrency txConcurrency;

    /** */
    private TransactionIsolation txIsolation;

    /** */
    private long txTimeout;

    private ConcurrentHashMap<Integer, AtomicLong> putGetCnt = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, AtomicLong> putAllGetAllCnt = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, AtomicLong> removeCnt = new ConcurrentHashMap<>();

    public AbstractTxTask(PocTesterArguments args) {
        super(args);
    }

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public AbstractTxTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /**
     * @return {@code Callable} Transaction body object.
     */
    protected abstract Callable<TxInfo> getTxBody();

    /**
     * @return {@code Object} Task lock object.
     */
    protected abstract Object getLock();

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        txTimeout = props.getLong("txTimeout", DFLT_TX_TIMEOUT);

        commitProb = props.getInteger("commitProb", DEFAULT_COMMIT_PROBABILITY);

        txConcurrency = TransactionConcurrency.valueOf(props.getString("txConcurrency", DEFAULT_TX_CONCURRENCY));

        txIsolation = TransactionIsolation.valueOf(props.getString("txIsolation", DEFAULT_TX_ISOLATION));

        probRange = props.getString("probRange", "100:0:0");

        putGetTxLenProbs = props.getString("putGetTxLenProbs", "1");

        putAllTxLenProbs = props.getString("putAllTxLenProbs", "1");

        removeTxProbs = props.getString("removeTxProbs", "1");
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        probRangeArr = getProbArray(probRange);

        putGetProbArr = getProbArray(putGetTxLenProbs);

        putAllProbArr = getProbArray(putAllTxLenProbs);

        removeTxProbArr = getProbArray(removeTxProbs);

        // https://ggsystems.atlassian.net/browse/IGN-11193
        // Touch all caches before starting transactional load
        for (String cacheName : getCacheNameList())
            ignite().cache(cacheName);

        //waitForLoad(LOG);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        sleep(200L);

        if (stopOnStopFlags()) {
            LOG.warn("Found stop flag. The task will stop.");
            tearDown();
        }

        if (!checkWaitFlags())
            tearDown();

        long futTimeout = txTimeout > 60_000 ? txTimeout - 60_000 : 60_000;

        ExecutorService serv = Executors.newFixedThreadPool(1);

        String tName = Thread.currentThread().getName();

        int idx = extractThreadIndex(tName);

        Future<TxInfo> fut = serv.submit(new TxRunner(idx));

        TxInfo res = null;

        try {
            res = fut.get(futTimeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to execute transaction.", e);
        }
        catch (TimeoutException ignored) {
            LOG.warn(String.format("Transaction has not been completed in %d seconds", 240));

            synchronized (getLock()) {
                takeThreadDump();
            }
        }

        try {
            res = fut.get(120_000L, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to execute transaction.", e);
        }
        catch (TimeoutException e) {
            LOG.warn(String.format("Transaction has not been completed in %d seconds", 360), e);
        }

        ConcurrentHashMap<Integer, AtomicLong> mapToWork = putGetCnt;

        if (res != null && res.commit()) {
            if (res.useGetAll)
                mapToWork = putAllGetAllCnt;

            if (res.useRemove)
                mapToWork = removeCnt;

            Integer txLen = res.getTxLen();

            AtomicLong val = mapToWork.putIfAbsent(txLen, new AtomicLong(1));

            if (val != null)
                val.getAndIncrement();
        }

        serv.shutdown();
    }

    @Override public void tearDown() {
        LOG.info("Transactions with get() and put():");

        for (Integer txLen : new TreeSet<>(putGetCnt.keySet()))
            LOG.info(String.format("Tx length = %d; Count = %d;", txLen, putGetCnt.get(txLen).get()));

        LOG.info("Transactions with getAll() and putAll():");

        for (Integer txLen : new TreeSet<>(putAllGetAllCnt.keySet()))
            LOG.info(String.format("Tx length = %d; Count = %d;", txLen, putAllGetAllCnt.get(txLen).get()));

        LOG.info("Transactions with removeAll() and putAll():");

        for (Integer txLen : new TreeSet<>(removeCnt.keySet()))
            LOG.info(String.format("Tx length = %d; Count = %d;", txLen, removeCnt.get(txLen).get()));

        super.tearDown();
    }

    /**
     * @param igniteTx Ignite transaction.
     * @param txConcurrency Transaction concurrency.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws Exception If failed.
     */
    private TxInfo doInTransaction(IgniteTransactions igniteTx, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation, Callable<TxInfo> clo) throws Exception {
        while (true) {
            final long txTmt = txTimeout;

            int calcTxLen = txLenMode.equals(DFLT_TX_LEN_MODE) ? txLenNum : calcTxLen();

            try (Transaction tx = igniteTx.txStart(txConcurrency, txIsolation, txTmt, calcTxLen)) {
                TxInfo res = clo.call();

                int chance = COMMON_RANDOM.nextInt(100);

                if (chance < getCommitProbability() && res.commit()) {
                    try {
                        tx.commit();
                    }
                    catch (CacheException e) {
                        LOG.error(String.format("Failed to commit transaction: %s; Will rollback.", e.getMessage()));

                        res.commit(false);

                        tx.rollback();
                    }
                }
                else {
                    res.commit(false);

                    tx.rollback();
                }

                return res;
            }
            catch (IllegalStateException e) {
                if (e.getMessage().contains("Getting affinity for topology version earlier than affinity is calculated"))
                    waitForAffinity();

                else
                    throw e;
            }
            catch (IgniteCacheRestartingException e) {
                LOG.warn(e.getMessage());

                sleep(5000L);
            }
            catch (CacheException e) {
                if (e.getCause() instanceof ClusterTopologyException) {
                    ClusterTopologyException topEx = (ClusterTopologyException)e.getCause();

                    topEx.retryReadyFuture().get();
                }
                else
                    throw e;
            }
            catch (ClusterTopologyException e) {
                e.retryReadyFuture().get();
            }
            catch (TransactionRollbackException | TransactionOptimisticException ignore) {
                // Safe to retry right away.
            }
            catch (IgniteTxTimeoutCheckedException | TransactionTimeoutException e) {
                LOG.error(String.format("Timeout (%d sec) is exceeded.", txTmt / 1000L), e);
            }
        }
    }

    /** */
    private int calcTxLen() {
        if (!txLenMode.equals("random"))
            throw new IllegalArgumentException("Unexpected txLenMode value");

        if (minTxLen >= txLenNum)
            throw new IllegalArgumentException("Defined min tx length should be less than defined tx length.");

        int delta = new Random().nextInt(txLenNum - minTxLen);

        return minTxLen + delta;
    }

    /**
     * @return Transaction commit probability.
     */
    protected int getCommitProbability() {
        return DEFAULT_COMMIT_PROBABILITY;
    }

    /** {@inheritDoc} */
    protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "entry");
        hdrMap.put("data", "latency");
        hdrMap.put("ops", "operations");

        propMap.put("reportDir", "eventstore");
    }

    protected List<TxPair> getPairList(int[] probArr, boolean twoCachesOnly) {
        int idx = COMMON_RANDOM.nextInt(probArr.length);

        int calcTxLen = probArr[idx];

        assert calcTxLen > 0;

        final List<TxPair> keyList = new ArrayList<>(calcTxLen);

        int threadNum = extractThreadIndex(Thread.currentThread().getName());

        List<Long> usedKeys = randomUniqKeys(calcTxLen * 2);

        String[] cacheNames = getRandomCacheNamesPair();

        for (int i = 0; i < calcTxLen; i++) {
            keyList.add(new TxPair(usedKeys.get(i * 2), usedKeys.get(i * 2 + 1), cacheNames[0], cacheNames[1]));

            if (!twoCachesOnly) {
                if (cachePairSameBackups) {
                    int backupsPrev = ((IgniteEx) ignite()).context().cache().cacheConfiguration(cacheNames[0]).getBackups();
                    int backups = -1;

                    do {
                        cacheNames = getRandomCacheNamesPair();
                        backups = ((IgniteEx) ignite()).context().cache().cacheConfiguration(cacheNames[0]).getBackups();
                    } while (backupsPrev != backups);

                }
                else
                    cacheNames = getRandomCacheNamesPair();
            }
        }

        assert !keyList.isEmpty();

        if (totalIterCnt.get() % 10_000 == 0) {
            LOG.info("Created following key list for transaction:");

            for (TxPair txPair : keyList)
                LOG.info(txPair.toString());
        }

        return keyList;
    }

    protected class TxInfo {
        private List<TxPair> involvedKeys;

        private int txLen;

        private boolean commit;

        private boolean useGetAll;

        private boolean useRemove;

        public TxInfo(List<TxPair> involvedKeys, boolean commit, boolean useGetAll, boolean useRemove) {
            this.involvedKeys = involvedKeys;
            this.commit = commit;
            this.useGetAll = useGetAll;
            this.useRemove = useRemove;

            txLen = involvedKeys.size();
        }

        public List<TxPair> getInvolvedKeys() {
            return involvedKeys;
        }

        public int getTxLen() {
            return txLen;
        }

        public void commit(boolean commit) {
            this.commit = commit;
        }

        public boolean commit() {
            return commit;
        }

        public boolean isUseGetAll() {
            return useGetAll;
        }

        public boolean isUseRemove() {
            return useRemove;
        }
    }

    private class TxRunner implements Callable<TxInfo> {
        private int threadIdx;

        public TxRunner(int threadIdx) {
            this.threadIdx = threadIdx;
        }

        @Override public TxInfo call() {
            Thread.currentThread().setName("TxThread-threadNum-" + threadIdx);

            try {
                return doInTransaction(ignite().transactions(), txConcurrency, txIsolation, getTxBody());
            }
            catch (Exception e) {
                LOG.error("Failed to complete transaction.", e);

                sleep(1000L);
            }

            return null;
        }
    }
}
