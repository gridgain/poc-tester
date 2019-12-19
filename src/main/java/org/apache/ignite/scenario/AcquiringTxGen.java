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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.scenario.internal.AbstractProcessingTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.model.processing.AcquiringTx;
import org.apache.ignite.scenario.internal.model.processing.AcquiringTxKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class AcquiringTxGen extends AbstractProcessingTask {
    private static final Logger LOG = LogManager.getLogger(AcquiringTxGen.class.getName());

    private static final String SQL_SELECT_ACCOUNT = "SELECT acctId FROM " + ACCOUNTS_TABLE_NAME +
            " WHERE (pan = ?);";

    private static final String SQL_INSERT_TX = "INSERT INTO " + ACQUIRING_TX_TABLE_NAME +
            " (ts, tgtPan, srcPan, tgtAcct, srcAcct, amount, reconciled, replicated) VALUES (?, ?, ?, ?, ?, ?, false, false);";

    public AcquiringTxGen(PocTesterArguments args) {
        super(args);
    }

    public AcquiringTxGen(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    @Override
    protected void body0() throws Exception {
        long cardNumSource = getRandomCardNum();
        long cardNumTarget;

        long amount = ThreadLocalRandom.current().nextInt(10, 1000 + 1);

        do {
            cardNumTarget = getRandomCardNum();
        } while (cardNumTarget == cardNumSource);

        // Perform collocated SQL queries to find out account IDs by their card numbers
        Long sourceAccount = ignite().compute().affinityCall(ACCOUNTS_CACHE_NAME, cardNumSource,
                new GetAccountTask(cardNumSource));

        Long targetAccount = ignite().compute().affinityCall(ACCOUNTS_CACHE_NAME, cardNumTarget,
                new GetAccountTask(cardNumTarget));

        LOG.debug("Source card: {}, source account: {}", cardNumSource, sourceAccount);
        LOG.debug("Target card: {}, target account: {}", cardNumTarget, targetAccount);

        assert sourceAccount != null : "Cannot find account ID for card " + cardNumSource;
        assert targetAccount != null : "Cannot find account ID for card " + cardNumTarget;

        // Inflate a TX object and save it
        IgniteCache<AcquiringTxKey, AcquiringTx> txCache = ignite().cache(ACQUIRING_TX_CACHE_NAME);

        long ts = Instant.now().toEpochMilli();

        SqlFieldsQuery sqlInsertTx = new SqlFieldsQuery(SQL_INSERT_TX)
                .setArgs(ts, cardNumTarget, cardNumSource, targetAccount, sourceAccount, amount);
        txCache.query(sqlInsertTx);
    }

    @Override
    protected String reportString() throws Exception {
        return null;
    }

    private static class GetAccountTask implements IgniteCallable<Long> {
        private Long cardNum;

        @IgniteInstanceResource
        private Ignite ignite;

        public GetAccountTask(Long cardNum) {
            this.cardNum = cardNum;
        }

        @Override
        public Long call() throws Exception {
            SqlFieldsQuery qrySelectAcct = new SqlFieldsQuery(SQL_SELECT_ACCOUNT)
                    .setArgs(cardNum)
                    .setLocal(true);
//                    .setPartitions(ignite.affinity(CARDHOLDERS_CACHE_NAME).partition(cardNum));

            List<List<?>> res = ignite.cache(ACCOUNTS_CACHE_NAME).query(qrySelectAcct).getAll();

            Long acct = null;

            for (List<?> row : res) {
                acct = (Long) row.get(0);
            }

            return acct;
        }
    }
}
