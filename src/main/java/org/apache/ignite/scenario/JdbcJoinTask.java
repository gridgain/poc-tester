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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.scenario.internal.AbstractJdbcTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Runs query from file. Parameters: - connectionString - JDBC Thin Driver connection string. - queryN - params matches
 * pattern will be treated as files with SQL queries that should be run.
 */
public class JdbcJoinTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(JdbcJoinTask.class.getName());

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
    public JdbcJoinTask(PocTesterArguments args, TaskProperties props) {
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
            LOG.error("Failed to write report: " + e.getMessage(), e);
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

    /**
     *
     */
    private void check1() {
        String qry = "select t.org_id, o.id, t.first_name, t.last_name from Taxpayers t left join Organisation o " +
            "on t.org_id=o.id order by t.org_id";

        LOG.debug(String.format("Running query: %s", qry));

        try (PreparedStatement stmt = getConnection().prepareStatement(qry)) {
            ResultSet rs = stmt.executeQuery();

            ResultSetMetaData rsmd = rs.getMetaData();

            int colNum = rsmd.getColumnCount();

            Map<IgnitePair<Integer>, Integer> map = new HashMap<>();

            while (rs.next()) {
                Integer orgId = null;

                Integer id = null;

                for (int j = 1; j <= colNum; j++) {
//                    if (j > 1)
//                        reportWriter.write(",  ");

                    String colName = rsmd.getColumnName(j);

                    if (colName.equals("ORG_ID"))
                        orgId = rs.getInt(j);

                    if (colName.equals("ID"))
                        id = rs.getInt(j);

                    String colVal = rs.getString(j);

//                    reportWriter.write(rsmd.getColumnName(j) + "=" +colVal);

                }

                IgnitePair<Integer> pair = new IgnitePair<>(orgId, id);

                Integer val = map.get(pair);

                if (!map.containsKey(pair))
                    map.put(pair, 1);
                else
                    map.put(pair, val + 1);
            }

            cntOp.getAndAdd(1);

            for (IgnitePair<Integer> pair : map.keySet()) {
                Integer exp = pair.get1() * 10;

                if (!exp.equals(map.get(pair))) {
                    reportWriter.write(String.format("Found possible error after running query: %s\n Unexpected number: " +
                        " of rows in result set: %d instead of %d\n", qry, exp, map.get(pair)));

                    reportWriter.flush();

                    cntEr.getAndAdd(1);
                }
            }

            reportWriter.flush();

        }
        catch (SQLException e) {
            if (cntLogEr.getAndIncrement() < 100)
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
