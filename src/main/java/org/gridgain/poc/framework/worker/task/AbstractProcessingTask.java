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

package org.gridgain.poc.framework.worker.task;

import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractProcessingTask extends AbstractReportingTask {

    public static final String CARDHOLDERS_TABLE_NAME = "Cardholders";

    public static final String CARDHOLDERS_CACHE_NAME = CARDHOLDERS_TABLE_NAME + "Cache";

    public static final String ACCOUNTS_TABLE_NAME = "Accounts";

    public static final String ACCOUNTS_CACHE_NAME = ACCOUNTS_TABLE_NAME + "Cache";

    public static final String AUTH_HISTORY_TABLE_NAME = "AuthHistory";

    public static final String AUTH_HISTORY_CACHE_NAME = AUTH_HISTORY_TABLE_NAME + "Cache";

    public static final String ACQUIRING_TX_TABLE_NAME = "AcquiringTx";

    public static final String ACQUIRING_TX_CACHE_NAME = ACQUIRING_TX_TABLE_NAME + "Cache";

    protected static final long DFLT_CLEANUP_THRESHOLD_SECONDS = 5;

    protected long cardNumStart;

    protected long cardNumEnd;

    public AbstractProcessingTask(PocTesterArguments args) {
        super(args);
    }

    public AbstractProcessingTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    @Override
    public void init() throws IOException {
        super.init();

        // Get cardNumRange from properties
        String cardsRange = props.getString("cardsRange", "0:9");

        cardNumStart = Long.parseLong(cardsRange.split(":")[0]);
        cardNumEnd = Long.parseLong(cardsRange.split(":")[1]);
    }

    public long getRandomCardNum() {
        return ThreadLocalRandom.current().nextLong(cardNumStart, cardNumEnd + 1);
    }

    /**
     * Get several distinct card numbers from records currently present in the acquiring transactions cache.
     * @param limit      maximum number of cards to get
     * @param reconciled filter by 'reconciled' flag
     * @param replicated filter by 'replicated' flag
     * @return a list of cards
     */
    public List<Long> getCardsNumFromTx(int limit, boolean reconciled, boolean replicated) {
        List<Long> cards = new ArrayList<>();

        String sqlSelectCard = "SELECT DISTINCT tgtPan FROM " + ACQUIRING_TX_TABLE_NAME +
                " WHERE (reconciled = ? AND replicated = ?) LIMIT ?";

        SqlFieldsQuery qry = new SqlFieldsQuery(sqlSelectCard);
        qry.setArgs(reconciled, replicated, limit);

        List<List<?>> qryRes = ignite().cache(ACQUIRING_TX_CACHE_NAME).query(qry).getAll();

        for (List<?> row : qryRes) {
            cards.add((Long) row.get(0));
        }

        return cards;
    }
}
