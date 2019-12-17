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
public class JdbcSelectTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(JdbcSelectTask.class.getName());

    /** */
    private int orgCnt;

    private long lastCntOp;

    private AtomicInteger cntOp;

    private AtomicInteger cntLogEr;

    private AtomicInteger cntEr;

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public JdbcSelectTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        cntOp = new AtomicInteger();

        cntLogEr = new AtomicInteger();

        cntEr = new AtomicInteger();

        super.setUp();

        if(!checkTable("Taxpayers"))
            throw new Exception("Table 'Taxpayers' is not exist or empty.");

    }

    /** {@inheritDoc} */
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
        if(reportWriter == null)
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

        int total = (int)(dataRangeTo - dataRangeFrom);

        int totalPossibleSum = getTotalNum((int) dataRangeFrom, (int) dataRangeTo);

        int totalLimit = (totalPossibleSum / 100) * limit;

        int org1shift = r.nextInt(total - 1);

        int org1 = (int) dataRangeFrom + org1shift;

        int org2shift = org1shift + r.nextInt(total - org1shift);

        int org2 = (int) dataRangeFrom + org2shift;

        while (getTotalNum(org1, org2) > totalLimit)
            org2 = org2 - 1;

//        LOG.info(String.format("Total = %d, Limit = %d Counted = %d", totalPossibleSum, limit, getTotalNum(org1, org2)));

        String qry = String.format("SELECT first_name, last_name FROM Taxpayers WHERE org_id >= %d AND org_id <= %d ORDER BY last_name ASC", org1, org2);

//        LOG.info(String.format("Running query: %s", qry));

        try (PreparedStatement stmt = getConnection().prepareStatement(qry)) {

            ResultSet rs;

            int cnt = 0;

            rs = stmt.executeQuery();

            ResultSetMetaData rsmd = rs.getMetaData();

            int colNum = rsmd.getColumnCount();

            String lastFound = null;

            while (rs.next()) {
                cnt++;

                for (int j = 1; j <= colNum; j++) {
//                    if (j > 1)
//                        System.out.print(",  ");

                    String colName = rsmd.getColumnName(j);

                    if(colName.equals("LAST_NAME")){
                        String curr = rs.getString(j);

                        if(lastFound != null){
                            if(lastFound.compareTo(curr) > 0){
                                reportWriter.write(String.format("Found possible error after running query: %s\n " +
                                    "Unexpected %s column value: previous value was '%s'; current value is '%s'\n",
                                    qry, colName, lastFound, curr));

                                reportWriter.flush();

                                cntEr.getAndAdd(1);
                            }
                        }

                        lastFound = curr;
                    }

                    String colVal = rs.getString(j);

//                    System.out.print(colVal + " = " + rsmd.getColumnName(j));
                }

//                System.out.println("");
            }

            int totalSum = getTotalNum(org1, org2);

            if(cnt != totalSum){
                reportWriter.write(String.format("Found possible error after running query: %s\n Unexpected result set " +
                    "size: %d instead of %d\n", qry, cnt, totalSum));

                reportWriter.flush();

                cntEr.getAndAdd(1);
            }


//            System.out.println("");

            cntOp.getAndAdd(1);

        }
        catch (SQLException e) {
            if(cntLogEr.getAndIncrement() < 100)
                LOG.error("Failed to execute query: " + qry, e);
        }
        catch (IOException e) {
            LOG.error("Failed to write report.", e);
        }

    }

    private int getTotalNum(int start, int fin) {
        int sum = ((start * 10 + fin * 10) * ((fin - start) + 1)) / 2;

        return sum;

    }
}
