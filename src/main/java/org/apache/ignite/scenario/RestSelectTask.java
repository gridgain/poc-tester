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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.scenario.internal.AbstractJdbcTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;

/**
 * Runs query from file. Parameters: - connectionString - JDBC Thin Driver connection string. - queryN - params matches
 * pattern will be treated as files with SQL queries that should be run.
 */
public class RestSelectTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(RestSelectTask.class.getName());

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
    public RestSelectTask(PocTesterArguments args, TaskProperties props){
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
        try {
            check1();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
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

    private void check1() throws IOException {

        List<String> servHosts = PocTesterUtils.getHostList(args.getServerHosts(), true);

        Collections.shuffle(servHosts);

        String host = servHosts.get(0);

        String[] colNames = new String[]{"id", "org_id", "first_name", "last_name", "gender", "state", "city", "univ"};

        List<String> colNameList = Arrays.asList(colNames);

        Collections.shuffle(colNameList);

        for(String colName : colNameList) {
            String url = String.format("http://%s:8080/ignite", host) + "?cmd=qryfldexe&pageSize=10&cacheName=SQL_PUBLIC_TAXPAYERS&qry=SELECT%20" + colName + "%20FROM%20Taxpayers";

            URLConnection conn = new URL(url).openConnection();

            conn.connect();

            try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
                ObjectMapper objMapper = new ObjectMapper();
                Map<String, Object> myMap = objMapper.readValue(streamReader,
                    new TypeReference<Map<String, Object>>() {
                    });

                if(myMap.get("error") != null)
                    LOG.error(String.format("Query %s has returned not null error response: %s", url, myMap.get("error")));

                if(cntOp.get() % 1000 == 0)
                    for(String key : myMap.keySet())
                        LOG.info(String.format("%s -> %s", key, myMap.get(key)));
            }
        }

        cntOp.getAndIncrement();
    }

    private int getTotalNum(int start, int fin) {
        int sum = ((start * 10 + fin * 10) * ((fin - start) + 1)) / 2;

        return sum;

    }
}
