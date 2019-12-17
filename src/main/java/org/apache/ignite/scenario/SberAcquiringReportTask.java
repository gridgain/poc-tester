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

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.scenario.internal.AbstractSberbankTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.List;

/**
 * Count and check sum of balances on all accounts.
 */
public class SberAcquiringReportTask extends AbstractSberbankTask {
    private static final Logger LOG = LogManager.getLogger(SberAcquiringReportTask.class.getName());

    private static final String SQL_SELECT_BALANCE_SUM = "SELECT SUM (balance) FROM " + ACCOUNTS_TABLE_NAME;

    private static final String SQL_SELECT_AUTH_SIZE = "SELECT COUNT(*) FROM " + AUTH_HISTORY_TABLE_NAME;

    private static final String SQL_SELECT_TX_SIZE = "SELECT COUNT(*) FROM " + ACQUIRING_TX_TABLE_NAME;

    private BigDecimal initialSum;

    public SberAcquiringReportTask(PocTesterArguments args) {
        super(args);
    }

    public SberAcquiringReportTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    @Override
    protected String reportString() throws Exception {
        return null;
    }

    @Override
    protected void body0() throws Exception {
        LOG.info("Iteration start");

        BigDecimal currentSum = new BigDecimal(0);

        List<List<?>> res = ignite().cache(ACCOUNTS_CACHE_NAME).query(new SqlFieldsQuery(SQL_SELECT_BALANCE_SUM)).getAll();

        for (List<?> row : res)
            currentSum = (BigDecimal) row.get(0);

        if (initialSum != null) {
            if (initialSum.equals(currentSum)) {
                LOG.info("Current sum equals initial sum: {}", currentSum);
            }
            else
                LOG.error("Current sum does not equal initial sum! Last: {}, current: {}", initialSum, currentSum);
        }
        else {
            initialSum = currentSum;
            LOG.info("Initial sum: {}", initialSum);
        }

        // Get size of authorization history table
        res = ignite().cache(AUTH_HISTORY_CACHE_NAME).query(
                new SqlFieldsQuery(SQL_SELECT_AUTH_SIZE)
                ).getAll();

        if (res != null && res.size() > 0)
            LOG.info("Auth history size: {}", (Long) res.get(0).get(0));
        else
            LOG.warn("Cannot get size of table {}!", AUTH_HISTORY_TABLE_NAME);

        // Get size of acquiring transactions table
        res = ignite().cache(ACQUIRING_TX_CACHE_NAME).query(
                new SqlFieldsQuery(SQL_SELECT_TX_SIZE)
        ).getAll();

        if (res != null && res.size() > 0)
            LOG.info("Acquiring TX size: {}", (Long) res.get(0).get(0));
        else
            LOG.warn("Cannot get size of table {}!", ACQUIRING_TX_TABLE_NAME);

        LOG.info("Iteration end");

        PocTesterUtils.sleep(interval);
    }
}
