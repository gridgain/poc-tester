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

package org.gridgain.poc.framework.worker.task.utils;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gridgain.poc.framework.utils.PocTesterUtils;

public class TxCounter implements IgniteCallable<Integer> {
    /** */
    private static final Logger LOG = LogManager.getLogger(TxCounter.class.getName());
    @IgniteInstanceResource
    private Ignite ignite;

    @Override public Integer call() throws Exception {
        IgniteKernal ik = (IgniteKernal)ignite;

        IgniteTxManager tm = ik.context().cache().context().tm();

        Collection<IgniteInternalTx> txList = tm.activeTransactions();

        LOG.info(String.format("Number of running transactions = %d", txList.size()));

        for(IgniteInternalTx tx : txList)
            LOG.info(String.format("Transaction start time = %s", PocTesterUtils.dateTime(tx.startTime())));


        return txList.size();
    }
}
