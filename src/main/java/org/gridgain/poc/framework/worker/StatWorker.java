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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;

import static org.gridgain.poc.framework.utils.PocTesterUtils.hms;
import static org.gridgain.poc.framework.utils.PocTesterUtils.println;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printw;
import static org.gridgain.poc.framework.utils.PocTesterUtils.sleep;

/**
 * Works with node statistics.
 */
public class StatWorker extends AbstractStartWorker {
    /** */
    private static final int DFLT_LINE_NUM = 5;

    /** */
    private ConcurrentHashMap<String, Map<String, List<String>>> servRepMap;

    /** */
    private ConcurrentHashMap<String, Map<String, List<String>>> clientRepMap;

    /** */
    private ConcurrentHashMap<String, Map<String, List<String>>> zkRepMap;

    /** */
    private static final String LOG_PREFIX = "-DpocLogFileName=";

    /** */
    private static final String JMX_PORT_PREFIX = "-Dcom.sun.management.jmxremote.port=";

    /** */
    private static final String JMX_PORT_MSG = "JMX remote port: ";

    /** */
    private static final String EMPTY_LOG_MSG = "Log file '%s' is empty. Probably process didn't even start correctly.";

    /** */
    private static final String CHECK_LINE_TEMPLATE = "cat %s | grep -i -E '%s' ";

    /** */
    private static final String LINES_IN_LOG_NUM = "Number of '%s' lines in log file '%s': %d";

    /** */
    private static final String FREE_SPACE_TEMPL = "cd %s; df --output=avail -h \"$PWD\" | tail -n 1";

    /** */
    private static final String PS_STAT_CMD = "ps -p PID_PLACEHOLDER -o pcpu,pmem,sz,drs,rss,trs,vsz";

    /** */
    private AtomicInteger servErrNum = new AtomicInteger();

    /** */
    private AtomicInteger clientErrNum = new AtomicInteger();

    /** */
    private String dateTime;

    /** Field to keep track of processes for which stats file is already been created. */
    private Collection<String> procPids = new HashSet<>();

    /**
     * Constructor.
     */
    public StatWorker() {
        uniqHosts = true;
        startThreads = Runtime.getRuntime().availableProcessors();

        servRepMap = new ConcurrentHashMap<>();
        clientRepMap = new ConcurrentHashMap<>();
        zkRepMap = new ConcurrentHashMap<>();

        dateTime = PocTesterUtils.dateTime();
    }

    /**
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        StatWorker worker = new StatWorker();

        worker.work(args);

        if (worker.isMonitor())
            worker.printMonitorResult();
    }

    /**
     * Print help.
     */
    @Override protected void printHelp() {
        System.out.println(" Collects stats from remote hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" stats.sh <options>");
        System.out.println();
        System.out.println(" Without any options will perform simple cluster state monitoring.");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -dst  || --dstat           <start|stop> Option to start or stop dstat.");
        System.out.println(" -pst  || --processStats    Flag to collect process statistics.");
        System.out.println(" -su   || --sudo            Flag for terminating dstat with super user privilege.");
        System.out.println(" -gl   || --grepLines       <Comma separated key words> Option to monitoring cluster and searching for specified words in log files.");
        System.out.println(" -du   || --discUsage       Flag to dump disc usage statistics.");
        System.out.println(" -td   || --threadDump      Flag to take thread dumps.");
        System.out.println(" -tr   || --timeToRun       <seconds> Time interval to work. If not defined only one iteration will be executed. Can be used only with -td or -du options.");
        System.out.println(" -ti   || --timeInterval    <seconds> Time interval between iterations. Default - 30 seconds. Can be used only with -td or -du options.");
        System.out.println();
    }

    /** {@inheritDoc} */
    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getAllHosts(args, uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {
        // Make directories for statistics data.
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        for (String host : getHostList(args)) {
            Collection<String> mkdirCmds = new ArrayList<>(3);

            if (args.isDiscUsage())
                mkdirCmds.add(String.format("mkdir -p %s/log/stats/disk-usage", rmtHome));

            if (args.isThreadDump())
                mkdirCmds.add(String.format("mkdir -p %s/log/stats/thread-dumps", rmtHome));

            if (args.procStat())
                mkdirCmds.add(String.format("mkdir -p %s/log/stats/process-stats", rmtHome));

            for (String mkdirCmd : mkdirCmds) {
                try {
                    worker.runCmd(host, mkdirCmd);
                }
                catch (Exception e) {
                    LOG.error(String.format("Failed to run command '%s' on the host '%s'", mkdirCmd, host), e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void work(String[] argsArr) {
        handleArgs(argsArr);

        beforeWork(args);

        workForTimeInterval(args);
    }

    /**
     * @param args Arguments.
     */
    private void workForTimeInterval(PocTesterArguments args) {
        boolean stop = false;

        long runStartTime = System.currentTimeMillis();

        while (!stop) {
            long iterStartTime = System.currentTimeMillis();

            workOnHosts(args);

            long iterFinTime = System.currentTimeMillis();

            stop = args.getTimeToRun() == 0 || iterFinTime > runStartTime + args.getTimeToRun() * 1000L;

            if (!stop) {
                long iterTime = iterFinTime - iterStartTime;

                long longInterval = args.getTimeInterval() * 1000L;

                if (iterTime < longInterval)
                    sleep(longInterval - iterTime);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total,
        String consID) {
        try {
            startOnHost(args, host);
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to get stats from the host '%s'", host), e);
        }
    }

    /**
     * @return {@code true} if monitoring is enabled or {@code false} otherwise.
     */
    private boolean isMonitor() {
        return (!args.isDiscUsage()
            && !args.isThreadDump()
            && !args.procStat()
            && (args.getDstat() == null)) || args.getGrepLines() != null;
    }

    /**
     * @param args Arguments.
     * @param host Host.
     * @throws Exception If failed.
     */
    private void startOnHost(PocTesterArguments args, String host) throws Exception {
        if (args.getDstat() != null)
            dstat(args, host);

        if (args.isDiscUsage())
            dumpDiscUsage(args, host);

        if (args.isThreadDump())
            takeThreadDumps(args, host);

        if (args.procStat())
            collectProcessStats(host);

        if (isMonitor()) {
            if (PocTesterUtils.getHostList(args.getServerHosts(), true).contains(host))
                monitor(args, host, NodeType.SERVER);

            if (PocTesterUtils.getHostList(args.getClientHosts(), true).contains(host))
                monitor(args, host, NodeType.CLIENT);

            if (PocTesterUtils.getHostList(args.getZooKeeperHosts(), true).contains(host))
                monitor(args, host, NodeType.ZOOKEEPER);
        }
    }

    /**
     * Starts or stops dstat on remote host.
     *
     * @param args Arguments.
     * @param host Host.
     * @throws Exception If failed.
     */
    private void dstat(PocTesterArguments args, String host) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String dstatArg = args.getDstat().toLowerCase();

        if (!dstatArg.equals("stop") && !dstatArg.equals("start")) {
            printw(String.format("Unknown argument for starting dstat: '%s'. Will not perform any actions.",
                args.getDstat()));

            return;
        }

        String killDstatCmd = args.isSudo() ? "sudo pkill -9 dstat" : "pkill -9 dstat";

        String killIostatCmd = args.isSudo() ? "sudo pkill -9 iostat" : "pkill -9 iostat";

        if (dstatArg.equals("stop")) {
            println(String.format("Terminating dstat on the host %s", host));

            worker.runCmd(host, killDstatCmd);

            worker.runCmd(host, killIostatCmd);
        }

        if (dstatArg.equals("start")) {
            println(String.format("Terminating dstat on the host %s", host));

            worker.runCmd(host, killDstatCmd);

            println(String.format("Starting dstat on the host %s", host));

            String dstatDir = String.format("%s/log/stats/dstat", rmtHome);

            if (!worker.exists(host, dstatDir))
                worker.runCmd(host, "mkdir -p " + dstatDir);

            String dateTime = PocTesterUtils.dateTime();

            String tmpName = String.format("%s/%s.sh", dstatDir, dateTime);

            //Create bash script on remote host than start it and delete.
            worker.runCmd(host, String.format("echo \"#!/usr/bin/env bash\" > %s;", tmpName));

            String startCmd = String.format("dstat --epoch --cpu -m --disk --io --net --sys --tcp --unix --output " +
                "%s/dstat-%s.log 1 > /dev/null &", dstatDir, dateTime);

            worker.runCmd(host, String.format("echo \"%s\" >> %s;", startCmd, tmpName));
            worker.runCmd(host, String.format("chmod +x %s;", tmpName));
            worker.runCmd(host, String.format("%s;", tmpName));
            worker.runCmd(host, String.format("rm -f %s;", tmpName));

            String startIostatCmd = String.format("iostat -t -x 1 > %s/iostat-%s.log &", dstatDir, dateTime);

            worker.runCmd(host, String.format("echo \"%s\" >> %s;", startIostatCmd, tmpName));
        }
    }

    /**
     * Dumps disc usage statistics on remote host.
     *
     * @param args Arguments.
     * @param host Host.
     * @throws Exception If failed.
     */
    private void dumpDiscUsage(PocTesterArguments args, String host) throws Exception {
        System.out.println(String.format("%sDumping disk usage statistic on the host %s", hms(), host));

        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String dateTime = PocTesterUtils.dateTime();

        String remDuStatDir = String.format("%s/log/stats/disk-usage", rmtHome);

        String findCmd = String.format("find %s -name \"*\" -type d", rmtHome);

        List<String> subDirs = worker.runCmd(host, findCmd);

        List<String> cmds = new ArrayList<>();

        for (String subDir : subDirs)
            cmds.add(String.format("du -msh %s >> %s/%s-disc-usage.log", subDir, remDuStatDir, dateTime));

        worker.runListCmd(host, cmds);
    }

    /**
     * Takes thread dumps on remote host.
     *
     * @param args Arguments.
     * @param host Host.
     * @throws Exception If failed.
     */
    public void takeThreadDumps(PocTesterArguments args, String host) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String dateTime = PocTesterUtils.dateTime();

        String remTdStatDir = String.format("%s/log/stats", args.getRemoteWorkDir());

        Map<String, String> pidMap = worker.getPidMap(host, null);

        if (pidMap.isEmpty())
            println(String.format("No poc-tester processes was found on the host %s", host));

        for (String pid : pidMap.keySet()) {
            String jstackCmd = String.format("jstack %s > %s/%s-%s.thread.dump", pid, remTdStatDir, dateTime,
                pidMap.get(pid));

            println(String.format("Taking thread dump of the process with consistent id %s on the host %s",
                pidMap.get(pid), host));

            worker.runCmd(host, jstackCmd);
        }
    }

    /**
     * Collects process statistics.
     *
     * @param host Host.
     * @throws Exception If failed.
     */
    private void collectProcessStats(String host) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String procStatDir = String.format("%s/log/stats/process-stats", args.getRemoteWorkDir());

        Map<String, String> pidMap = worker.getPidMap(host, null);

        if (pidMap.isEmpty())
            println(String.format("No poc-tester processes was found on the host %s", host));

        for (String pid : pidMap.keySet()) {
            boolean fileExists = procPids.contains(pid);

            procPids.add(pid);

            String psCmd = PS_STAT_CMD.replace("PID_PLACEHOLDER", pid);

            List<String> psRes = worker.runCmd(host, psCmd);

            String fileName = String.format("%s/%s-%s.stats", procStatDir, dateTime, pidMap.get(pid));

            if (psRes.size() > 1) {
                if (!fileExists) {
                    String hdr = "timestamp " + psRes.get(0);

                    writeToFile(worker, host, fileName, hdr);
                }

                writeToFile(worker, host, fileName, psRes.get(1));
            }
        }
    }

    /**
     * Writes string to specified file on remote host.
     *
     * @param worker SSH worker.
     * @param host Host.
     * @param fileName File name.
     * @param src Source string
     * @throws Exception If failed.
     */
    private void writeToFile(RemoteSshExecutor worker, String host, String fileName, String src)
        throws Exception {
        String res = convert(src);

        String echoCmd = String.format("echo '%s' >> %s", res, fileName);

        worker.runCmd(host, echoCmd);
    }

    /**
     * Convert string output of 'ps' command into comma separated string.
     *
     * @param src Source string.
     * @return Result string
     */
    private String convert(String src) {
        while (src.startsWith(" "))
            src = src.substring(1);

        while (src.contains("  "))
            src = src.replace("  ", " ");

        if (!src.startsWith("timestamp"))
            src = System.currentTimeMillis() + " " + src;

        return src.replace(" ", ",");
    }

    /**
     * Monitoring nodes state.
     *
     * @param args Arguments.
     * @param host Host.
     * @throws Exception If failed.
     */
    private void monitor(PocTesterArguments args, String host, NodeType type) throws Exception {
        println("Starting monitor for host: " + host);

        Map<String, List<String>> hostRepMap = new HashMap<>();

        Collection<String> exploredFiles = new HashSet<>();

        String freeSpaceCmd = String.format(FREE_SPACE_TEMPL, rmtHome);

        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        Map<String, String> pidMap = worker.getPidMap(host, type);

        for (String pid : pidMap.keySet()) {
            List<String> pidRepList = new ArrayList<>();

            String psCmd = String.format("ps -p %s -o comm,args=ARGS", pid);

            String consId = pidMap.get(pid);

            List<String> argList = worker.runCmd(host, psCmd);

            for (String arg : argList) {
                if (type == NodeType.CLIENT && arg.contains("clientDirName"))
                    pidRepList.add("Task name: " + getTaskName(arg));

                String[] argArr = arg.split(" ");

                for (String a : argArr) {

                    if (a.startsWith(JMX_PORT_PREFIX))
                        pidRepList.add(JMX_PORT_MSG + a.replace(JMX_PORT_PREFIX, ""));

                    if (a.startsWith(LOG_PREFIX)) {
                        String logPath = a.replace(LOG_PREFIX, "");

                        if (worker.exists(host, logPath)) {
                            findLines(worker, host, logPath, pidRepList);

                            exploredFiles.add(logPath);
                        }
                        else
                            printw(String.format("Failed to find log file %s, on the host %s", logPath, host));
                    }
                }
            }

            hostRepMap.put("Consistent id: " + consId, pidRepList);
        }

        if (worker.exists(host, rmtHome)) {
            List<String> resp = worker.runCmd(host, freeSpaceCmd);

            if (!resp.isEmpty()) {
                hostRepMap.put(String.format("Disc space available for 'work' directory: %s", resp.get(0)),
                    new ArrayList<>());
            }
            else
                printw(String.format("Failed to get free space from the host %s", host));

            if (args.getWalPath() != null && worker.exists(host, args.getWalPath())) {
                String spaceCmd = String.format(FREE_SPACE_TEMPL, args.getWalPath());

                List<String> respWAL = worker.runCmd(host, spaceCmd);

                if (!resp.isEmpty()) {
                    hostRepMap.put(String.format("Disc space available for WAL (%s): %s", args.getWalPath(),
                        respWAL.get(0)), new ArrayList<>());
                }
                else
                    printw(String.format("Failed to get free space from the host %s", host));

            }

            if (args.getWalArch() != null && worker.exists(host, args.getWalArch())) {
                String spaceCmd = String.format(FREE_SPACE_TEMPL, args.getWalArch());

                List<String> respArch = worker.runCmd(host, spaceCmd);

                if (!resp.isEmpty()) {
                    hostRepMap.put(String.format("Disc space available for WAL archive (%s): %s", args.getWalArch(),
                        respArch.get(0)), new ArrayList<>());
                }
                else
                    printw(String.format("Failed to get free space from the host %s", host));

            }
        }
        else
            printw(String.format("Failed to get free space from the host %s. Remote work directory %s was not found.",
                host, rmtHome));

        if (args.getGrepLines() != null && type != NodeType.ZOOKEEPER) {
            String rmtLog = String.format("%s/log", rmtHome);

            if (worker.exists(host, rmtLog)) {

                String findCmd = String.format("find %s -name \"poc-tester-%s-%s-*.log\" -type f", rmtLog, type, host);

                List<String> logFileList = worker.exists(host, rmtLog) ? worker.runCmd(host, findCmd) :
                    new ArrayList<>(0);

                List<String> deadLogList = new ArrayList<>();

                for (String logPath : logFileList) {
                    if (!exploredFiles.contains(logPath)) {
                        findLines(worker, host, logPath, deadLogList);

                        exploredFiles.add(logPath);
                    }
                }

                if (!deadLogList.isEmpty())
                    hostRepMap.put("Data from no longer running processes:", deadLogList);
            }
            else
                printw(String.format("No log directory was found on the host %s", host));
        }

        if (type == NodeType.SERVER && !hostRepMap.isEmpty())
            servRepMap.put(host, hostRepMap);

        int alive = 0;

        for (String key : hostRepMap.keySet())
            if (key.startsWith("Consistent"))
                alive++;

        hostRepMap.put(String.format("Alive %s nodes: %d", type, alive), new ArrayList<>());

        if (type == NodeType.CLIENT && !hostRepMap.isEmpty())
            clientRepMap.put(host, hostRepMap);

        if (type == NodeType.ZOOKEEPER && !hostRepMap.isEmpty())
            zkRepMap.put(host, hostRepMap);
    }

    /**
     *
     * @param worker SSH worker.
     * @param host Host.
     * @param logPath Log file path.
     * @param dstList List to fill.
     * @throws Exception If failed.
     */
    private void findLines(RemoteSshExecutor worker, String host, String logPath, Collection<String> dstList) throws Exception {
        String fileName = new File(logPath).getName();

        String parentName = new File(logPath).getParent();

        String checkSizeCmd = "head -1 " + logPath;

        if (worker.runCmd(host, checkSizeCmd).isEmpty()) {
            dstList.add(String.format("Log parent directory: %s", parentName));

            dstList.add(String.format(EMPTY_LOG_MSG, fileName));

            return;
        }

        for (String line : linesToLook) {
            String checkLineCmd = String.format(CHECK_LINE_TEMPLATE, logPath, line);

            List<String> lineList = worker.runCmd(host, checkLineCmd);

            if (!lineList.isEmpty()) {
                if (line.toLowerCase().contains("error")) {
                    if (fileName.contains("server"))
                        updateServErrNum(lineList.size());

                    if (fileName.contains("client"))
                        updateClientErrNum(lineList.size());
                }

                dstList.add(String.format("Log parent directory: %s", parentName));

                dstList.add(String.format(LINES_IN_LOG_NUM, line.replace("|", "' or '"), fileName, lineList.size()));

                int lines = lineList.size() >= DFLT_LINE_NUM ? DFLT_LINE_NUM : lineList.size();

                String templ = lines > 1 ? String.format("Last %d lines:", lines) : "Last line:";

                dstList.add(templ);

                for (int i = lineList.size() - lines; i < lineList.size(); i++)
                    dstList.add(String.format("    %s", lineList.get(i)));
            }
        }
    }

    /**
     *
     */
    private void printMonitorResult() {
        List<String> servHostList = getSortedHosts(servRepMap);
        List<String> clientHostList = getSortedHosts(clientRepMap);
        List<String> zkHostList = getSortedHosts(zkRepMap);

        if (!servRepMap.isEmpty())
            println("Server hosts:");
        int runningServNum = 0;
        for (String host : servHostList)
            runningServNum += printHostResult(host, NodeType.SERVER);

        if (!clientRepMap.isEmpty())
            println("Client hosts:");
        int runningClientNum = 0;
        for (String host : clientHostList)
            runningClientNum += printHostResult(host, NodeType.CLIENT);

        if (!zkRepMap.isEmpty())
            println("ZooKeeper hosts:");
        int runningZkNum = 0;
        for (String host : zkHostList)
            runningZkNum += printHostResult(host, NodeType.ZOOKEEPER);

        if (!servRepMap.isEmpty())
            println(String.format("Server nodes running: %d out of %d",
                    runningServNum,
                    PocTesterUtils.getHostList(args.getServerHosts(), false).size()));
        if (!clientRepMap.isEmpty())
            println(String.format("Client nodes running: %d", runningClientNum));
        if (!zkRepMap.isEmpty())
            println(String.format("ZooKeeper nodes running: %d out of %d",
                    runningZkNum,
                    PocTesterUtils.getHostList(args.getZooKeeperHosts(), false).size()));

        if (servErrNum.get() > 0)
            println(String.format("Error lines in server log files: %d", servErrNum.get()));
        if (clientErrNum.get() > 0)
            println(String.format("Error lines in client log files: %d", clientErrNum.get()));
    }

    /**
     *
     * @param host Host.
     * @param type Node type.
     * @return Number of running nodes.
     */
    private int printHostResult(String host, NodeType type) {
        println(String.format("    %s:", host));

        ConcurrentHashMap<String, Map<String, List<String>>> map = null;
        switch (type) {
            case SERVER:
                map = servRepMap;
                break;
            case ZOOKEEPER:
                map = zkRepMap;
                break;
            default:
                map = clientRepMap;
                break;
        }

        Map<String, List<String>> idMap = map.get(host);

        List<String> idList = PocTesterUtils.getList(idMap.keySet());

        int res = 0;

        for (String consId : idList) {
            println(String.format("        %s", consId));

            if (consId.startsWith("Consistent"))
                res++;

            for (String str : idMap.get(consId))
                println(String.format("            %s", str));
        }

        return res;
    }

    /**
     *
     * @param map Host map.
     * @return Sorted hosts.
     */
    private List<String> getSortedHosts(Map<String, Map<String, List<String>>> map) {
        List<String> res = new ArrayList<>();

        for (Map.Entry<String, Map<String, List<String>>> entry : map.entrySet()) {
            String host = entry.getKey();

            res.add(host);
        }

        Collections.sort(res);

        return res;
    }

    /**
     *
     * @param arg Task arguments used to start task.
     * @return Task name.
     */
    private String getTaskName(String arg) {
        String[] argArr = arg.split(" ");

        for (String a : argArr) {
            if (a.startsWith("-DclientDirName")) {
                String[] lineArr = a.split("-");

                return lineArr[lineArr.length - 1];
            }
        }
        return "Unknown";
    }

    /**
     *
     * @param host Host.
     * @return Expected number of nodes.
     */
    private int getNodesNum(String host) {
        int res = 0;

        List<String> servHostList = PocTesterUtils.getHostList(args.getServerHosts(), false);

        for (String servHost : servHostList)
            if (servHost.equals(host))
                res++;

        return res;
    }

    /**
     * @param delta Delta.
     */
    private void updateServErrNum(int delta) {
        servErrNum.getAndAdd(delta);
    }

    /**
     * @param delta Delta.
     */
    private void updateClientErrNum(int delta) {
        clientErrNum.getAndAdd(delta);
    }
}

