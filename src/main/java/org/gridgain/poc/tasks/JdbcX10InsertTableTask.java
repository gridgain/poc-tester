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

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.gridgain.poc.framework.worker.task.AbstractJdbcTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runs query from file. Parameters: - connectionString - JDBC Thin Driver connection string. - queryN - params matches
 * pattern will be treated as files with SQL queries that should be run.
 */
public class JdbcX10InsertTableTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(JdbcX10InsertTableTask.class.getName());

    /** */
    private int orgCnt;

    /** */
    private long lastCntOp;

    /** */
    private AtomicInteger cntOp;

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public JdbcX10InsertTableTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        setTaskLoad();

//        orgCnt = props.getInteger("orgCnt", 0);
//        LOG.debug("orgCNT = " + orgCnt);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        cntOp = new AtomicInteger();

        if(lockFile != null) {
            LOG.info("lockFile = " + lockFile);

            createLockFile();
        }
        super.setUp();
    }

    @Override protected Logger log() {
        return LOG;
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        for (long i = taskLoadFrom; i <= taskLoadTo; i++) {
            List<String> qrs = qryGen.generate(i);

            LOG.debug(String.format("list size = %d", qrs.size()));

            for(String qry : qrs)
                LOG.debug("Query = " + qry);

            for (int j = 0; j < qrs.size(); j++) {
                String qry = qrs.get(j);

                if (j < 3 || j > qrs.size() - 3)
                    LOG.info(String.format("Running query: %s", qry));

                try (PreparedStatement stmt = getConnection().prepareStatement(qry)) {

                    ResultSet rs;

                    int cnt = 0;

                    if (!qry.startsWith("INSERT")) {
                        rs = stmt.executeQuery();

                        ResultSetMetaData rsmd = rs.getMetaData();

                        int colNum = rsmd.getColumnCount();
                        while (rs.next())
                            cnt++;

                        LOG.info("Resultset size: " + cnt);
                    }
                    else
                        stmt.execute();

                    cntOp.getAndAdd(1);

                    //LOG.info(String.format("After executing query: %s", qry));

                }
                catch (SQLException e) {
                    LOG.error("Failed to execute query: " + qry);
                    e.printStackTrace();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String getTaskReport() {
        if(reportWriter == null)
            return null;

        int currCnt = cntOp.get();

        long delta = currCnt - lastCntOp;

        lastCntOp = currCnt;

        long timeStamp = System.currentTimeMillis();

        try {
            reportWriter.write(timeStamp + "," + delta + "\n");

            reportWriter.flush();
        }
        catch (IOException e) {
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
        hdrMap.put("data", "delta");
//        hdrMap.put("ops", "operations");

        propMap.put("reportDir", "reports");
    }

    /**
     * Runs after body has finished.
     *
     * Delete lock file.
     */
    @Override public void tearDown() {
        File toDel;

        if (lockFile != null) {
            toDel = new File(homeDir + SEP + lockFile);

            if(toDel.exists())
                toDel.delete();
        }

        super.tearDown();
    }

    /** */
    private void setTaskLoad(){
        totalNodesNum = args.getTotalNodesNum();

        if(totalNodesNum == 0){
            LOG.error("Total nodes number can't be 0!");
            tearDown();
        }

        nodeCntr = args.getNodeCntr();

        long totalOrgCnt = dataRangeTo - dataRangeFrom + 1;

        long totalTaxPayersCnt = ((dataRangeFrom * 10 + dataRangeTo * 10) * totalOrgCnt) / 2;

        if (args.getTotalNodesNum() == 1) {
            taskLoadFrom = dataRangeFrom;
            taskLoadTo = dataRangeTo;

            String msg = String.format("JdbcX10InsertTableTask is starting insert. Will insert organisations from " +
                "%d to %d. (%d members)", taskLoadFrom, taskLoadTo, totalTaxPayersCnt);

            System.out.println(msg);

            LOG.info(msg);
        }
        else {
            long taskTaxPayersCnt = totalTaxPayersCnt / totalNodesNum;

            long lastDefOrg = dataRangeFrom;

            long lastDefUser = 0;

            for(int i = 0; i < totalNodesNum; i++){
                long loopStart = taskTaxPayersCnt * i;

                long loopEnd = (loopStart + taskTaxPayersCnt) > totalTaxPayersCnt ? totalTaxPayersCnt :
                    loopStart + taskTaxPayersCnt ;

                taskLoadFrom = lastDefOrg == dataRangeFrom ? lastDefOrg : lastDefOrg + 1;

                while(loopEnd > lastDefUser && lastDefOrg < dataRangeTo){
                    lastDefOrg++;

                    lastDefUser = lastDefUser + lastDefOrg * 10;

                    LOG.debug(String.format("lastDefOrg = %d", lastDefOrg));
                    LOG.debug(String.format("taskLoadTo = %d", taskLoadTo));
                }

                taskLoadTo = lastDefOrg;

                if(nodeCntr == i)
                    break;
            }

            long userCntForLoad = ((taskLoadFrom * 10 + taskLoadTo * 10) * ((taskLoadTo - taskLoadFrom) + 1)) / 2;

            String msg = taskLoadFrom <= taskLoadTo ? String.format("JdbcX10InsertTableTask %d of %d starting insert. " +
                "Will insert organisations from %d to %d. (%d members)", nodeCntr + 1, totalNodesNum, taskLoadFrom,
                taskLoadTo, userCntForLoad) :
                "JdbcX10InsertTableTask will not insert any data (all the data should be inserted by previously started tasks).";

            System.out.println(msg);

            LOG.info(msg);
        }
    }

}
