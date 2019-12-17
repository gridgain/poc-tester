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

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.scenario.internal.AbstractJdbcTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;

/**
 * Runs query from file. Parameters: - connectionString - JDBC Thin Driver connection string. - queryN - params matches
 * pattern will be treated as files with SQL queries that should be run.
 */
public class JdbcRandomTxTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(JdbcRandomTxTask.class.getName());

    /** */
    private int orgCnt;

    private long lastCntOp;

    private AtomicInteger cntOp;

    private AtomicInteger cntLogEr;

    private AtomicInteger cntEr;

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public JdbcRandomTxTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        cntOp = new AtomicInteger();

        cntLogEr = new AtomicInteger();

        cntEr = new AtomicInteger();

        super.setUp();
    }

    @Override protected Logger log() {
        return LOG;
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        check1();
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();
    }

    @Override public String getTaskReport() {
        if (reportWriter == null)
            return null;

        int currCnt = cntOp.get();

        int currEr = cntEr.get();

        lastCntOp = currCnt;

        try {
            reportWriter.write(String.format("[%s] Completed %d operations. Found %d possible errors.", dateTime(), currCnt,
                currEr));

            reportWriter.newLine();

            reportWriter.flush();
        }
        catch (IOException e) {
            LOG.error("Failed to write report: " + e.getMessage());
            e.printStackTrace();
        }

        //TODO: avoid null result.
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "entry");
        hdrMap.put("data", "latency");
        hdrMap.put("ops", "operations");

        propMap.put("reportDir", "eventstore");
    }

    private void check1() {

        Random r = new Random();

        long cardHolderId1 = getRandomKey();

        long cardHolderId2 = getRandomKey();

        while (cardHolderId1 == cardHolderId2)
            cardHolderId2 = getRandomKey();

        long amount1 = getValue(cardHolderId1);

        long amount2 = getValue(cardHolderId2);

        int delta = r.nextInt(1000);

        long newAmount1 = amount1 - delta;

        long newAmount2 = amount2 + delta;

        setAmount(newAmount1, cardHolderId1);

        setAmount(newAmount2, cardHolderId2);

        long num = getSeq();

        setTransaction(num, cardHolderId1, cardHolderId1, cardHolderId2, delta, 0);
    }

    /**
     *
     * @param id
     * @param card
     * @param cardFrom
     * @param cardTo
     * @param amount
     * @param atm
     */
    private void setTransaction(long id, long card, long cardFrom, long cardTo, long amount, long atm) {
        String txInsert = String.format("INSERT INTO Financial_Transactions(transaction_id, " +
                "amount, atm_id, card_number, card_number_transfer_from, card_number_transfer_to, currency_code, " +
                "merchant_id, transaction_type_code) VALUES(%d, %d, %d, %d, %d, %d, %d, %d, %d)",
            id, amount, atm, card, cardFrom, cardTo, 0, 0, 0);

        try (PreparedStatement stmt = getConnection().prepareStatement(txInsert)) {
            stmt.execute();
        }
        catch (SQLException e) {
            LOG.error("Failed to execute query: " + txInsert);
            e.printStackTrace();
        }

    }

    private Long getSeq() {
        String qry = String.format("SELECT \"sqlFunc\".getNext();");

        LOG.debug(String.format("Running query: %s", qry));

        try (PreparedStatement stmt = getConnection().prepareStatement(qry)) {

            ResultSet rs = stmt.executeQuery();

            ResultSetMetaData rsmd = rs.getMetaData();

            if (rs.next()) {

                Long colVal = rs.getLong("\"sqlFunc\".GETNEXT()");

                LOG.debug("CNTR = " + colVal);

                return colVal;
            }
            else
                return null;

        }
        catch (SQLException e) {
            if(cntLogEr.getAndIncrement() < 100)
                LOG.error("Failed to execute query: " + qry, e);

            return null;
        }
    }

    private Long getValue(long key) {
        String qry = String.format("SELECT amount FROM Accounts WHERE id = %d", key);

        LOG.debug(String.format("Running query: %s", qry));

        try (PreparedStatement stmt = getConnection().prepareStatement(qry)) {

            ResultSet rs = stmt.executeQuery();

            ResultSetMetaData rsmd = rs.getMetaData();

            if (rs.next()) {

                Long colVal = rs.getLong("amount");

                LOG.debug(colVal);

                return colVal;
            }
            else
                return null;

        }
        catch (SQLException e) {
            if (cntLogEr.getAndIncrement() < 100)
                LOG.error("Failed to execute query: " + qry, e);

            return null;
        }

    }

    private void setAmount(long val, long id) {
        String qry = String.format("UPDATE accounts SET amount = %d WHERE id = %d;", val, id);

        LOG.debug(String.format("Running query: %s", qry));

        try (PreparedStatement stmt = getConnection().prepareStatement(qry)) {
            stmt.execute();
        }
        catch (SQLException e) {
            if (cntLogEr.getAndIncrement() < 100)
                LOG.error("Failed to execute query: " + qry, e);
        }

        long newVal = getValue(id);

        System.out.println(newVal);
    }

    private int getTotalNum(int start, int fin) {
        int sum = ((start * 10 + fin * 10) * ((fin - start) + 1)) / 2;

        return sum;

    }
}
