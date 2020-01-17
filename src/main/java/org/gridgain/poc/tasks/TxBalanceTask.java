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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.gridgain.poc.framework.worker.task.AbstractTxTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.worker.task.utils.TxPair;
import org.gridgain.poc.framework.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Perform transactional balance transfer between two keys in two caches.
 *
 * There are 3 types of operations are used to make a transfer:
 * - get() + put().
 * - getAll() + putAll().
 * - getAll() + putAll() + removeAll(). In this case the task moves the value from "key" to it's reversed key.
 *   Reversed keys are calculated in the following way: {@code rKey = (key * -1) - 1}.
 *
 * Task properties:
 * - see {@link AbstractTxTask}.
 */
public class TxBalanceTask extends AbstractTxTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(TxBalanceTask.class.getName());

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public TxBalanceTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected Callable<TxInfo> getTxBody() {
        return new TxBody(ignite());
    }

    /** {@inheritDoc} */
    @Override protected Object getLock() {
        return TxBalanceTask.class;
    }

    /**
     * Transaction body.
     */
    private class TxBody implements Callable<TxInfo> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Constructor.
         *
         * @param ignite Ignite instance.
         */
        TxBody(final Ignite ignite) {
            this.ignite = ignite;
        }

        /** {@inheritDoc} */
        @Override public TxInfo call() throws Exception {
            int chance = COMMON_RANDOM.nextInt(100);

            if (chance < probRangeArr[0])
                return doTxPutGet(getPairList(putGetProbArr, false));
            else if (chance < probRangeArr[0] + probRangeArr[1])
                return doTxPutAllGetAll(getPairList(putAllProbArr, true));
            else
                return doTxRemoveAll(getPairList(removeTxProbArr, true));
        }

        /**
         * Transfer some balance between two keys with single puts and gets
         * @param pairList A list of key pairs
         * @return
         */
        private TxInfo doTxPutGet(List<TxPair> pairList) {
            boolean commit = true;

            for (TxPair pair : pairList) {
                String cacheName0 = pair.getCacheName0();
                String cacheName1 = pair.getCacheName1();

                IgniteCache<Long, SampleObject> cache0 = ignite().cache(cacheName0);
                IgniteCache<Long, SampleObject> cache1 = ignite().cache(cacheName1);

                long key0 = pair.getKey0();
                long key1 = pair.getKey1();

                SampleObject val0 = cache0.get(key0);
                SampleObject val1 = cache1.get(key1);

                if (val0 == null) {
                    key0 = reversedKey(key0);
                    val0 = cache0.get(key0);

                    if (val0 == null) {
                        LOG.error("Cache {} does not have neither key {} nor its reversed key",
                                cacheName0, key0);
                        stopAndCollect();
                    }
                }

                if (val1 == null) {
                    key1 = reversedKey(key1);
                    val1 = cache1.get(key1);

                    if (val1 == null) {
                        LOG.error("Cache {} does not have neither key {} nor its reversed key",
                                cacheName1, key1);
                        stopAndCollect();
                    }
                }

                int bal0 = val0.getBalance();
                int bal1 = val1.getBalance();
                int delta = COMMON_RANDOM.nextInt(100);

                val0 = val0.copy();
                val0.setBalance(bal0 - delta);

                val1 = val1.copy();
                val1.setBalance(bal1 + delta);

                ignite.cache(cacheName0).put(key0, val0);
                ignite.cache(cacheName1).put(key1, val1);
            }

            return new TxInfo(pairList, commit, false, false);
        }

        /**
         * Transfer some balance between two keys with putAll() and getAll()
         * @param pairList
         * @return
         */
        private TxInfo doTxPutAllGetAll(List<TxPair> pairList) {
            boolean commit = true;

            Map<Long, SampleObject> map0 = new HashMap<>();
            Map<Long, SampleObject> map1 = new HashMap<>();
            Set<Long> keySet0 = new HashSet<>();
            Set<Long> keySet1 = new HashSet<>();

            String cacheName0 = pairList.get(0).getCacheName0();
            String cacheName1 = pairList.get(0).getCacheName1();

            IgniteCache<Long, SampleObject> cache0 = ignite().cache(cacheName0);
            IgniteCache<Long, SampleObject> cache1 = ignite().cache(cacheName1);

            for (TxPair pair : pairList) {
                keySet0.add(pair.getKey0());
                keySet1.add(pair.getKey1());
            }

            // GetAll()
            map0 = cache0.getAll(keySet0);
            map1 = cache1.getAll(keySet1);

            for (TxPair pair : pairList) {
                long key0 = pair.getKey0();
                long key1 = pair.getKey1();

                SampleObject val0 = map0.get(key0);
                SampleObject val1 = map1.get(key1);

                if (val0 == null) {
                    map0.remove(key0);
                    key0 = reversedKey(key0);
                    val0 = cache0.get(key0);
                    map0.put(key0, val0);

                    if (val0 == null) {
                        LOG.error("Cache {} does not have neither key {} nor its reversed key",
                                cacheName0, key0);
                        stopAndCollect();
                    }
                }

                if (val1 == null) {
                    map1.remove(key1);
                    key1 = reversedKey(key1);
                    val1 = cache1.get(key1);
                    map1.put(key1, val1);

                    if (val1 == null) {
                        LOG.error("Cache {} does not have neither key {} nor its reversed key",
                                cacheName1, key1);
                        stopAndCollect();
                    }
                }

                int bal0 = val0.getBalance();
                int bal1 = val1.getBalance();
                int delta = COMMON_RANDOM.nextInt(100);

                val0 = val0.copy();
                val0.setBalance(bal0 - delta);

                val1 = val1.copy();
                val1.setBalance(bal1 + delta);

                map0.put(key0, val0);
                map1.put(key1, val1);
            }

            // PutAll()
            cache0.putAll(map0);
            cache1.putAll(map1);

            return new TxInfo(pairList, commit, true, false);
        }

        /**
         * Move all balance from a key to its reversed (negative) key with putAll() and removeAll()
         * @param pairList
         * @return
         */
        private TxInfo doTxRemoveAll(List <TxPair> pairList) {
            boolean commit = true;

            // KV for removal
            Map<Long, SampleObject> map0 = new HashMap<>();

            // KV with reversed keys to put
            Map<Long, SampleObject> map1 = new HashMap<>();

            String cacheName0 = pairList.get(0).getCacheName0();
            IgniteCache<Long, SampleObject> cache0 = ignite().cache(cacheName0);

            Set<Long> keySet0 = new HashSet<>();

            // Accumulate keys for GetAll()
            for (TxPair pair : pairList)
                keySet0.add(pair.getKey0());

            // GetAll()
            map0 = cache0.getAll(keySet0);

            for (Long key0 : map0.keySet()) {
                SampleObject val0 = map0.get(key0);

                if (val0 == null) {
                    Long rKey0 = reversedKey(key0);
                    val0 = cache0.get(rKey0);

                    if (val0 == null) {
                        LOG.error("Cache {} does not have neither key {} nor its reversed key",
                                cacheName0, key0);
                        stopAndCollect();
                    }

                    map0.remove(key0);
                    key0 = rKey0;
                    map0.put(key0, val0);
                }
            }

            for (Map.Entry<Long, SampleObject> entry : map0.entrySet()) {
                map1.put(
                        reversedKey(entry.getKey()),
                        entry.getValue()
                );
            }

            // Move key values to reversed keys
            cache0.removeAll(map0.keySet());
            cache0.putAll(map1);

            return new TxInfo(pairList, commit, true, true);
        }
    }
}
