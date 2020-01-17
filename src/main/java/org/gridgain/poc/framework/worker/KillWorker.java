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

package org.gridgain.poc.framework.worker;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;

import static org.gridgain.poc.framework.utils.PocTesterUtils.hms;

/**
 * Created by oostanin on 09.12.17.
 */
public class KillWorker extends AbstractStartWorker {

    public KillWorker() {
        super();

        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    public static void main(String[] args) {
        new KillWorker().work(args);
    }

    /**
     * Print help.
     */
    protected void printHelp(){
        System.out.println(" Kill nodes.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" kill-remote.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -id   || --nodeID          <node id> If not defined all nodes on defined hosts will be killed.");
        System.out.println(" -su   || --sudo            Flag for using super user privileges.");
        System.out.println(" -a    || --allJava         Flag for killing all java processes on defined hosts.");
        System.out.println(" -km   || --killMode        Kill mode, e.g. -2 or -9 etc.");
        System.out.println();
    }

    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getAllHosts(args, uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {

    }

    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total, String defConsId) {
        if (args.isKillAllJava()) {
            try {
                killAllJava(args, host);
            }
            catch (Exception e) {
                LOG.error(String.format("Failed to kill java processes on the host %s", host), e.getMessage());
            }

            return;
        }

//        long t = 0;
//
//        long e = 0;
//
//        if(host.equals("172.25.1.33"))
//            e = t / 0;

        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        if (defConsId == null && args.getNodeID() != null)
            defConsId = args.getNodeID();

        Map<String, String> pidMap = null;
        try {
            pidMap = worker.getPidMap(host, null);
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to kill java processes on the host %s", host), e.getMessage());

            return;
        }

        String msg = String.format("%sFound %d poc-tester processes on the host %s", hms(), pidMap.size(), host);

        System.out.println(msg);
        LOG.info(msg);

        for (String pid : pidMap.keySet()) {
            String cmd = getKillCmd(args, pid);

            String consId = pidMap.get(pid);

            String killMsg = String.format("%sKilling node with id %s on the host %s", hms(), consId, host);

            if (defConsId == null || consId.endsWith(defConsId)){
                System.out.println(killMsg);

                LOG.info(killMsg);

                try {
                    worker.runCmd(host, cmd);
                }
                catch (Exception e) {
                    LOG.error(String.format("Filed to kill process with pid %s on the host %s", pid, host));
                }
            }
        }
    }

    public void killOnHost(PocTesterArguments args, String host, String defConsId) {
        LOG.info(String.format("Killing node with id %s on the host %s", defConsId, host));

        start(args, host, null, 0, 0, defConsId);
    }

    public void killOnHostByPid(PocTesterArguments args, String host, String pid) {
        LOG.info("Killing process with PID {} on host {}", pid, host);

        String killCmd = getKillCmd(args, pid);
        RemoteSshExecutor sshWorker = new RemoteSshExecutor(args);

        try {
            sshWorker.runCmd(host, killCmd);
        }
        catch (Exception e) {
            LOG.error("Failed to kill process with PID {} on host {}", pid, host);
        }
    }

    private void killAllJava(PocTesterArguments args, String host) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        List<String> pidList = worker.runCmd(host, "pgrep 'java'");

        String pidSelf = new File("/proc/self").getCanonicalFile().getName();

        for (String pid : pidList) {
            if(pid.equals(pidSelf))
                continue;

            String cmd = getKillCmd(args, pid);

            String msg = String.format("%sKilling process with id %s on the host %s", hms(), pid, host);

            System.out.println(msg);

            LOG.info(msg);

            worker.runCmd(host, cmd);
        }
    }

    /**
     * Creates command that terminate processes. Depending on args.isSudo() argument it can be 'sudo kill' or just
     * 'kill'. Also depending on args.killMode() argument the command can be 'kill -9' (default) or 'kill -2', etc.
     *
     * @param args {@code PocTesterArguments} arguments.
     * @return {@code String} command to terminate processes.
     */
    private String getKillCmd(PocTesterArguments args, String pid){
        StringBuilder sb = new StringBuilder();

        if(args.isSudo())
            sb.append("sudo ");

        sb.append("kill ");

        sb.append(args.getKillMode());

        sb.append(" ").append(pid);

        return sb.toString();
    }
}

