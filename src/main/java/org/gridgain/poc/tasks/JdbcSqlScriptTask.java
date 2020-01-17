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

import org.gridgain.poc.framework.worker.task.AbstractJdbcTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.gridgain.poc.framework.utils.PocTesterUtils.getRandomFromSequence;
import static org.gridgain.poc.framework.worker.PrepareWorker.CFG_BACKUPS_PLACEHOLDER;

/**
 * Runs query from file. Parameters: - connectionString - JDBC Thin Driver conn string. - queryN - params matches
 * pattern will be treated as files with SQL queries that should be run.
 */
public class JdbcSqlScriptTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(JdbcSqlScriptTask.class.getName());

    /** */
    private static final Pattern QUERY_PARAMETER_NAME_VALIDATOR = Pattern.compile("^query\\d*");

    /** */
    private List<String> queries = new ArrayList<>();

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public JdbcSqlScriptTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        for (String qry : queries) {
            if (qry.matches(".*" + CFG_BACKUPS_PLACEHOLDER + ".*"))
                qry = qry.replaceAll(CFG_BACKUPS_PLACEHOLDER, getRandomFromSequence(args.getBackups()));

            try {
                LOG.info("Executing query: " + qry);
                getConnection().prepareStatement(qry).execute();
            }
            catch (SQLException e) {
                LOG.error("Failed to execute query: " + qry, e);
                throw new TestFailedException();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        for (Object key : props.keySet()) {
            if (QUERY_PARAMETER_NAME_VALIDATOR.matcher((CharSequence)key).matches()) {
                String val = props.getString((String)key);

                assert val != null;

                Path path = Paths.get(homeDir + SEP + val);

                assert Files.isRegularFile(path) : "File not found: " + path;

                try (BufferedReader br = new BufferedReader(new FileReader(homeDir + SEP + val))) {
                    String line;

                    while ((line = br.readLine()) != null)
                        queries.add(line);

                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        assert !queries.isEmpty() : "No queries found";
    }

    /** {@inheritDoc} */
    @Override protected Logger log() {
        return LOG;
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "entry");
        hdrMap.put("data", "latency");
        hdrMap.put("ops", "operations");

        propMap.put("reportDir", "reports");
    }
}
