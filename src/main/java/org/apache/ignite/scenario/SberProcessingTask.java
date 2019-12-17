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

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.scenario.internal.AbstractSberbankTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.jdbc.CardholdersGenerator;
import org.apache.ignite.scenario.internal.model.sberbank.AuthHistoryKey;
import org.apache.ignite.scenario.internal.model.sberbank.CardholderKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;


public class SberProcessingTask extends AbstractSberbankTask {
    private static final Logger LOG = LogManager.getLogger(SberProcessingTask.class.getName());

    // https://ggsystems.atlassian.net/browse/QA-2599 SELECT without 'LIMIT 1' can return >1 rows
    private static final String SQL_CARDHOLDER_SELECT =
            "SELECT " + CARDHOLDERS_TABLE_NAME + ".pan, firstName, lastName, acctId" +
                    " FROM " + CARDHOLDERS_TABLE_NAME + ", " + ACCOUNTS_TABLE_NAME +
                    " WHERE (" + CARDHOLDERS_TABLE_NAME + ".pan = ? AND " + ACCOUNTS_TABLE_NAME + ".pan = ?)" +
                    " LIMIT 1";

    private static final String SQL_HISTORY_INSERT = "INSERT INTO " + AUTH_HISTORY_TABLE_NAME +
            " (ts, srcPan, tgtPan, srcAcct, amount, resCode) VALUES (?, ?, ?, ?, ?, ?)";

    public SberProcessingTask(PocTesterArguments args) {
        super(args);
    }

    public SberProcessingTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    @Override
    protected String reportString() throws Exception {
        return null;
    }

    @Override
    protected void body0() throws Exception {
        LOG.debug("Iteration start");

        long cardNumSource = getRandomCardNum();

        long cardNumTarget = 0;

        // Get random target card number
        do {
            cardNumTarget = getRandomCardNum();
        } while (cardNumTarget == cardNumSource);

        // Calculate target account. TODO: refactor
        long targetAccount = cardNumTarget * CardholdersGenerator.CARD_TO_ACCOUNT_MULTIPLIER;

        LOG.debug("Source card number: {}", cardNumSource);
        LOG.debug("Target card number: {}", cardNumTarget);

        final long finalCardNumTarget = cardNumTarget;

        final long timestamp = Instant.now().toEpochMilli();

        // Collocate transaction inflation with cardholder's data
        ignite().compute().affinityRun(CARDHOLDERS_CACHE_NAME, cardNumSource, () -> {
                SqlFieldsQuery qryCardSelect = new SqlFieldsQuery(SQL_CARDHOLDER_SELECT)
                        .setArgs(cardNumSource, cardNumSource);
                qryCardSelect.setLocal(true);
                qryCardSelect.setPartitions(ignite().affinity(CARDHOLDERS_CACHE_NAME).partition(cardNumSource));

                List<List<?>> res = ignite().cache(CARDHOLDERS_CACHE_NAME).query(qryCardSelect).getAll();

                for (List<?> row : res) {
                    Object card = row.get(0);
                    Object firstName = row.get(1);
                    Object lastName = row.get(2);
                    Object sourceAccount = row.get(3);

                    LOG.debug("Card: {}, Name: {} {}, Source account: {}, Target account: {}",
                            card, firstName, lastName, sourceAccount, targetAccount);

                    long amount = ThreadLocalRandom.current().nextInt(10, 1000 + 1);
                    int txRes = 0;

                    try {
                        SqlFieldsQuery qryTxInsert = new SqlFieldsQuery(SQL_HISTORY_INSERT)
                                .setArgs(timestamp, cardNumSource, finalCardNumTarget, sourceAccount, amount, txRes);
                        qryTxInsert.setLocal(true);
                        qryTxInsert.setPartitions(ignite().affinity(CARDHOLDERS_CACHE_NAME).partition(cardNumSource));

                        ignite().cache(AUTH_HISTORY_CACHE_NAME).query(qryTxInsert);
                    }
                    catch (Exception e) {
                        LOG.error("Failed to insert data into table " + AUTH_HISTORY_TABLE_NAME, e);
                    }
                }
            }
        );

        // Ensure data for a cardholder and a transaction with the same card number reside in the same partition
        long partCardholder = ignite().affinity(CARDHOLDERS_CACHE_NAME).partition(new CardholderKey(cardNumSource));
        long partAuthHistory = ignite().affinity(AUTH_HISTORY_CACHE_NAME).partition(new AuthHistoryKey(timestamp, cardNumSource));

        LOG.debug("Partitions for card {}: ", cardNumSource);
        LOG.debug("- in Cardholders table: {}", partCardholder);
        LOG.debug("- in History table: {}", partAuthHistory);

        assert partCardholder == partAuthHistory : "Collocation failure";

        LOG.debug("Iteration end");
    }
}

