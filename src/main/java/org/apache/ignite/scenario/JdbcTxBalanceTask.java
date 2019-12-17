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
import java.util.Map;
import org.apache.ignite.scenario.internal.AbstractJdbcTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Runs query from file. Parameters: - connectionString - JDBC Thin Driver connection string. - queryN - params matches
 * pattern will be treated as files with SQL queries that should be run.
 */
public class JdbcTxBalanceTask extends AbstractJdbcTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(JdbcTxBalanceTask.class.getName());

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public JdbcTxBalanceTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();
    }

    @Override protected Logger log() {
        return LOG;
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        if (!checkWaitFlags())
            tearDown();

        LOG.warn("Current release does not support this task");

        sleep(interval);
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();
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
}
