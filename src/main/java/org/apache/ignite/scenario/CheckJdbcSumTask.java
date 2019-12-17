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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.scenario.internal.AbstractJdbcTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Checks sum in SQL table.
 */
public class CheckJdbcSumTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(CheckJdbcSumTask.class.getName());

    /** */
    private long lastTotalSum = 0;

    /** */
    private long startTime;

    /**
     * @param args
     * @param props
     */
    public CheckJdbcSumTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        startTime = System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        if (!checkWaitFlags())
            tearDown();

        LOG.info("Iteration start");

        raiseSyncFlags();

        sleep(10000);

        long currSum = countSum();

        if (lastTotalSum == 0)
            LOG.info("Initial sum is {}", currSum);
        else if (lastTotalSum != currSum) {
            LOG.error(String.format("Current sum %d is not equal last sum %d", currSum, lastTotalSum));
            stopAndCollect();
            throw new TestFailedException();
        }
        else
            LOG.info(String.format("Current sum %d equals last sum %d", currSum, lastTotalSum));

        lastTotalSum = currSum;

        clearSyncFlags();

        sleep(interval);
    }

    /**
     * @return
     */
    private long countSum() throws TestFailedException {
        LOG.info("Starting to count sum.");

        long currSum = 0;

        boolean done = false;

        ExecutorService execServ = null;

        while (!done) {
            try {
                execServ = Executors.newSingleThreadExecutor();

                Future<Long> fut = execServ.submit(new Checker());

                currSum = fut.get(300, TimeUnit.SECONDS);

                done = true;
            }
            catch (Exception e) {
                LOG.error(e.getMessage(), e);

                sleep(5000L);
            }
            finally {
                if (execServ != null)
                    execServ.shutdown();
            }

            long currTime = System.currentTimeMillis();

            if (currTime > startTime + timeToWork * 1000L) {
                LOG.info(String.format("Time to work (%d seconds) is exceeded. Will not try to count sum any longer.",
                        timeToWork));

                tearDown();
            }
        }

        return currSum;
    }

    public String getReportString() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "number");
        hdrMap.put("data", "total_sum");
    }

    /**
     *
     */
    private class Checker implements IgniteCallable<Long> {

        @Override public Long call() throws Exception {
            String qry = String.format("SELECT SUM(balance) FROM Balance;");

            long res = 0;

            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet rs = null;

            try {
                conn = getConnection();
                conn.setAutoCommit(false);

                stmt = conn.prepareStatement(qry);
                rs = stmt.executeQuery();

                conn.commit();

                ResultSetMetaData rsmd = rs.getMetaData();

                while (rs.next()) {
                    for (int j = 1; j <= rsmd.getColumnCount(); j++) {
                        String colName = rsmd.getColumnName(j);

                        LOG.info(String.format("Col name = %s", colName));

                        if (colName.equals("SUM(BALANCE)"))
                            res = rs.getLong(j);
                    }
                }

                rs.close();
            }
            catch (SQLException e) {
                LOG.error("Failed to count sum", e);

                if (conn != null)
                    conn.rollback();
            }
            finally {
                if (conn != null & conn.isValid(1))
                    conn.setAutoCommit(true);

                if (stmt != null)
                    stmt.close();
            }

            return res;
        }
    }

    @Override protected Logger log() {
        return LOG;
    }
}
