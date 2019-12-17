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

import org.apache.ignite.scenario.internal.AbstractTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.ignite.scenario.internal.utils.SSHCmdWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RemoteCmdTask extends AbstractTask {
    private static final Logger LOG = LogManager.getLogger(RemoteCmdTask.class.getName());

    private final String REMOTE_WORK_DIR_PLACEHOLDER = "REMOTE_WORK_DIR";

    private final String REMOTE_HOST_PLACEHOLDER = "REMOTE_HOST";

    private String remoteCmd;

    public RemoteCmdTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);

        remoteCmd = props.getString("remoteCmd");
    }

    @Override
    public @Nullable String getTaskReport() {
        return null;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        if (remoteCmd == null) {
            LOG.error("Please specify 'remoteCmd' in properties file.");
            System.exit(1);
        }

        remoteCmd = remoteCmd.replace(REMOTE_WORK_DIR_PLACEHOLDER, args.getRemoteWorkDir());

        LOG.info("Remote command to execute: {}", remoteCmd);
    }

    @Override
    protected void body0() throws InterruptedException, TestFailedException {
        LOG.info("Iteration started.");

        // Get hosts to work on
        List<String> hosts = PocTesterUtils.getPocServerHosts(args, true);

        // Create parallel executor service by number of hosts
        ExecutorService service = Executors.newFixedThreadPool(hosts.size());

        // Add futures to executor service
        List<Future> futs = new ArrayList<>();

        for (String host : hosts) {
            futs.add(service.submit(new RemoteCmdWorker(host)));
        }

        // Wait for all futures to end
        for (Future fut : futs) {
            try {
                fut.get();
            }
            catch (Exception e) {
                LOG.error("Failed to get result of a future", e);
            }
        }

        service.shutdown();

        // Sleep for some time
        PocTesterUtils.sleep(interval);
    }

    @Override
    protected boolean disableClient() {
        return true;
    }

    private class RemoteCmdWorker implements Callable<String> {
        private String host;

        private RemoteCmdWorker(String host) {
            this.host = host;
        }

        @Override
        public String call() throws Exception {
            SSHCmdWorker worker = new SSHCmdWorker(args);

            String remoteCmdCurr = remoteCmd.replace(REMOTE_HOST_PLACEHOLDER, host);

            LOG.debug("Command to execute:");
            LOG.debug(" - host:    {}", host);
            LOG.debug(" - command: {}", remoteCmdCurr);

            List<String> outLines = worker.runCmd(host, remoteCmdCurr);

            LOG.debug("Result output:");
            for (String line : outLines) {
                LOG.debug(line);
            }

            return null;
        }
    }
}
