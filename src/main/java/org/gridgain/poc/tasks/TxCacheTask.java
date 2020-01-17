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

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.gridgain.poc.framework.worker.task.AbstractTxTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.worker.task.utils.TxPair;
import org.gridgain.poc.framework.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Do the following within a transaction:
 * <p>
 * - select 2 random keys from 2 random caches;
 * - get value from the first key;
 * - put it into the second key.
 */
public class TxCacheTask extends AbstractTxTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(TxCacheTask.class.getName());

    /** */
    private static final int DEFAULT_COMMIT_PROBABILITY = 100;

    /** */
    protected int txLen = DEFAULT_TX_LEN;

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public TxCacheTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected Callable<TxInfo> getTxBody() {
//        return new TxBody(ignite(), getUniqKeys(txLen * 2));
        return new TxBody(ignite());
    }

    /** {@inheritDoc} */
    @Override protected Object getLock() {
        return TxCacheTask.class;
    }

    /**
     * Transaction body.
     */
    private class TxBody implements Callable<TxInfo> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

//        /** */
//        private final Map<Long, String> uniqKeys;
//
        /**
         * Constructor.
         *
         * @param ignite Ignite instance.
         */
        TxBody(final Ignite ignite){
            this.ignite = ignite;
        }

        /** {@inheritDoc} */
        @Override public TxInfo call() throws Exception {
            final List<TxPair> pairList = getPairList(putGetProbArr, false);

            for (TxPair pair : pairList) {
                long key0 = pair.getKey0();
                long key1 = pair.getKey1();

                String cacheName0 = pair.getCacheName0();

                String cacheName1 = pair.getCacheName1();

                SampleObject val = (SampleObject)ignite.cache(cacheName0).get(key0);

                if (val == null)
                    LOG.error("Failed to find value for key " + key0 + " in cache "
                        + cacheName0 + ". Make sure all data was loaded properly.");

                ignite.cache(cacheName1).put(key1, val);
            }

            return new TxInfo(pairList, true, false, false);
        }
    }
}
