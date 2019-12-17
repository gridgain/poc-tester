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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.ignite.scenario.internal.AbstractJdbcTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runs query from file. Parameters: - connectionString - JDBC Thin Driver conn string. - queryN - params matches
 * pattern will be treated as files with SQL queries that should be run.
 */
public class JdbcSqlQueryTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(JdbcSqlQueryTask.class.getName());

    /** */
    private static final Pattern QUERY_PARAMETER_NAME_VALIDATOR = Pattern.compile("^query\\d*");

    /** */
    private List<String> queries = new ArrayList<>();

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public JdbcSqlQueryTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        try (Connection conn = setupConnection()) {
            conn.setSchema("cachepoc0");

            for (String qry : queries) {
                try {
                    if (LOG.isDebugEnabled())
                        LOG.debug("Executing query: " + qry);

                    try (PreparedStatement stmt = conn.prepareStatement(qry)) {

                        ResultSet rs = stmt.executeQuery();

                        int cnt = 0;

                        while (rs.next())
                            cnt++;


                        if (LOG.isDebugEnabled())
                            LOG.debug("Resultset size: " + cnt);

                    }
                }
                catch (SQLException e) {
                    LOG.error("Failed to execute query: " + qry);
                    e.printStackTrace();
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
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

                byte[] content = Files.readAllBytes(path);

                String qry = new String(content);

                queries.add(qry);
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
