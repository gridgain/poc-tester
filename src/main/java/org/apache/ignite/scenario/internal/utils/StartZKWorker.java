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

package org.apache.ignite.scenario.internal.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.ignite.scenario.internal.PocTesterArguments;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;

/**
 * Created by oostanin on 09.12.17.
 */
public class StartZKWorker extends AbstractStartWorker {

    /** ZooKeeper's log file name (hardcoded in bin/zkServer.sh) */
    public static final String ZK_LOG_FILE = "zookeeper.out";

    public StartZKWorker() {
        super();

        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    public static void main(String[] args) {
        new StartZKWorker().work(args);
    }

    /**
     * Print help.
     */
    protected void printHelp() {
        System.out.println(" Starts zookeeper on remote hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" clean.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -wd   || --remoteWorkDir   <path to upload zip file and run poc-tester in>");
    }

    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getZooKeeperHosts(args, uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {

    }

    /** {@inheritDoc} */
    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total, String consID) {
        String rmtWorkDir = args.getRemoteWorkDir();
        String zkLogDir = String.format("%s/log/zookeeper", rmtWorkDir);
        String zkLog = String.format("%s/%s", zkLogDir, ZK_LOG_FILE);

        String zkStartCmd = String.format("SERVER_JVMFLAGS=-DCONSISTENT_ID=%s nohup %s/zookeeper/bin/zkServer.sh start 2>%s/start-error-%s.log >%s/start-%s.log",
            cntr, rmtWorkDir, zkLogDir, dateTime, zkLogDir, dateTime);

        println(String.format("Starting zookeeper on the host %s", host));

        SSHCmdWorker worker = new SSHCmdWorker(args);

        final String rmtZKDir = String.format("%s/zookeeper", rmtWorkDir);

        try {
            String zkIdPath = String.format("%s/data/myid", rmtZKDir);

            // Delete file with ZK ID
            if(worker.exists(host, zkIdPath))
                worker.runCmd(host, String.format("rm %s/%s", rmtZKDir, zkIdPath));

            // Rotate ZK log if present
            if (worker.exists(host, zkLog)) {
                String newLogFileName = zkLog.replace(
                        "." + FilenameUtils.getExtension(zkLog),
                        "-" + dateTime + ".log");
                LOG.info("Renaming ZK log file to {}", FilenameUtils.getName(newLogFileName));
                worker.runCmd(host, String.format("mv %s %s", zkLog, newLogFileName));
            }

            // Create file with ZK ID
            worker.runCmd(host,
                    String.format("echo %d > %s/data/myid", cntr + 1, rmtZKDir));

            // Start ZK
            worker.runCmd(host, zkStartCmd);
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to start zookeeper on the host %s", host), e);
        }

    }
}
