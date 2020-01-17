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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.jetbrains.annotations.NotNull;

import static org.gridgain.poc.framework.utils.PocTesterUtils.*;

/**
 * Class for deploying files to remote hosts.
 */
public class StartNodeWorker extends AbstractStartWorker {
    private final String PROPS_JVM_OPTS_PATH = "config/jvm-opts/jvm-opts.properties";

    private Properties jvmProperties;

    private NodeType nodeType;

    private String commonJvmOpts;

    private String serverJvmOpts;

    private List<String> serverJvmOptsRandom = new ArrayList<>();

    private String clientJvmOpts;

    private String jmxOpts;

    private List<String> clientJvmOptsRandom = new ArrayList<>();

    /** */
    public StartNodeWorker() {
        super();

        this.uniqHosts = false;
        this.startThreads = 1;

        jvmProperties = PocTesterUtils.loadProp(PROPS_JVM_OPTS_PATH);

        if (jvmProperties == null) {
            LOG.error("Cannot load property file {}", PROPS_JVM_OPTS_PATH);
            System.exit(1);
        }

        commonJvmOpts = jvmProperties.getProperty("COMMON_JVM_OPTS", "");
        serverJvmOpts = jvmProperties.getProperty("SERVER_JVM_OPTS", "");
        clientJvmOpts = jvmProperties.getProperty("CLIENT_JVM_OPTS", "");
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    /** */
    public static void main(String[] args) {
        new StartNodeWorker().work(args);
    }

    /**
     * Print help.
     */
    @Override protected void printHelp() {
        //NO_OP
    }

    @Override protected List<String> getHostList(PocTesterArguments args) {
        if(args.getTaskProperties() != null)
            return PocTesterUtils.getHostList(args.getClientHosts(), false);
        else
            return PocTesterUtils.getHostList(args.getServerHosts(), false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {
        if (!args.isNoConfigCheck()){
            if(args.getTaskProperties() != null)
                checkConfig(args, args.getClientCfg());
            else
                checkConfig(args, args.getServerCfg());
        }

        if (nodeType == null){
            if(args.getTaskProperties() != null)
                nodeType = NodeType.CLIENT;
            else
                nodeType = NodeType.SERVER;
        }
    }

    /** */
    @Override public void start(PocTesterArguments args, final String host, final String dateTime, int cntr, int total,
                                String consID) {
        final RemoteSshExecutor worker = new RemoteSshExecutor(args);

        final String startCmd;

        try {
            startCmd = getStartCmd(args, host, dateTime, cntr, total, consID);
        }
        catch (Exception e) {
            LOG.error("Failed to get start command.", e);

            return;
        }

        // Creating thread for starting node.
        ExecutorService serv = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, String.format("Node-starter-%s-%s", host, hms()));
            }
        });

        Future<List<String>> fut = serv.submit(new Callable<List<String>>() {
            @Override public List<String> call() throws Exception {

                return worker.runStartCmd(host, startCmd);
            }
        });

        List<String> res = new ArrayList<>();

        boolean started = false;

        int tryCnt = 0;

        int maxTry = 60;

        long initTimeOut = 120_000L;

        long tryInterval = 60_000L;

        // Waiting for node to respond when it is started and ready.
        // Taking thread dump of the node JVM if node is not started for 120 seconds and then each 60 seconds.
        while (!started && tryCnt++ < maxTry) {
            long timeout = tryCnt > 1 ? tryInterval : initTimeOut;

            long totalTime = (initTimeOut + (tryCnt - 1) * tryInterval) / 1000L;

            try {
                res = fut.get(timeout, TimeUnit.MILLISECONDS);

                started = true;
            }
            catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            catch (TimeoutException ignored) {
                LOG.warn(String.format("No response from node %s after %d seconds. Will take thread dump and wait for " +
                        "another %d seconds. Count = %d of %d", consID,  totalTime, tryInterval / 1000L, tryCnt, maxTry));

                StatWorker statWorker = new StatWorker();

                try {
                    statWorker.takeThreadDumps(args, host);
                }
                catch (Exception e1) {
                    LOG.error(String.format("Failed to take thread dumps on the host %s", host), e1);
                }
            }
        }

        serv.shutdown();

        if (res.isEmpty())
            printer(String.format("Failed to start node with consistent id %s on the host %s", consID, host));

        for (String str : res)
            System.out.println(str);
    }

    private String getStartCmd(PocTesterArguments args, final String host, final String dateTime, int cntr, int total,
        String consID) throws Exception {

        rmtHome = args.getRemoteWorkDir();

        final RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String taskCls;

        String nodeStartCls = null;

        String taskDirName = null;

        String dstCfg = null;

        String logDirName = null;

        String cfg = null;

        String libPath = rmtHome + "/libs";

        String pocLibPath = null;

        String startMsgPref = null;

        String targetJvmOpts = commonJvmOpts;

        String randomJvmOpt = "";

        switch (nodeType) {
            case CLIENT:
                nodeStartCls = "org.gridgain.poc.framework.starter.TaskStarter";

                taskCls = PocTesterUtils.readPropFromFile(args.getTaskProperties(), "MAIN_CLASS");

                targetJvmOpts += " " + clientJvmOpts;

                randomJvmOpt = getClientJvmOptsRandomNext();
                LOG.info("Random JVM option to append: {}", randomJvmOpt);
                targetJvmOpts += " " + randomJvmOpt;

                taskDirName = String.format("task-%s-%s", dateTime, taskCls);

                logDirName = String.format("/log/%s", taskDirName);

                String relCfgPath = args.getClientCfg().replace(rmtHome + "/", "");

                if(!relCfgPath.startsWith("config/")){
                    int idx = relCfgPath.indexOf("config/");

                    relCfgPath = relCfgPath.substring(idx, relCfgPath.length());
                }

                cfg = rmtHome + "/" + taskDirName + "/" + relCfgPath;

                LOG.info(String.format("rmtHome = %s", rmtHome));
                LOG.info(String.format("taskDirName = %s", taskDirName));
                LOG.info(String.format("relCfgPath = %s", relCfgPath));

                String srcCfg = Paths.get(locHome, "config").toString();

                dstCfg = String.format("%s/%s/config", rmtHome, taskDirName);

                String srcPocLibs = Paths.get(locHome, "poc-tester-libs").toString();

                pocLibPath = String.format("%s/%s/libs", rmtHome, taskDirName);

                worker.put(host, srcCfg, dstCfg);

                worker.put(host, srcPocLibs, pocLibPath);

                startMsgPref = String.format("Starting task %s", taskCls);

                break;

            case SERVER:
                nodeStartCls = "org.gridgain.poc.framework.starter.IgniteStarter";

                targetJvmOpts += " " + serverJvmOpts;

                randomJvmOpt = getServerJvmOptsRandomNext();
                LOG.info("Random JVM option to append: {}", randomJvmOpt);
                targetJvmOpts += " " + randomJvmOpt;

                logDirName = "/log/server";

                dstCfg = String.format("%s/config", rmtHome);

                cfg = args.getServerCfg().startsWith(rmtHome) ? args.getServerCfg() :
                    String.format("%s/%s", rmtHome, args.getServerCfg());

                pocLibPath = rmtHome + "/poc-tester-libs";

                startMsgPref = "Starting server node";

                break;
            default:
                LOG.error("Unknown node type: " + nodeType);

                break;
        }

        // A relative path to caches config to import from server/client node config
        Path cfgCacheRelPath = Paths.get(cfg).getParent().relativize(Paths.get(rmtHome, args.getImportedBaseCfg()));

        String remoteLogPath = rmtHome + logDirName;

        String mkLogDirCmd = "mkdir -p " + remoteLogPath;

        worker.runCmd(host, mkLogDirCmd);

        String unixStartPath = PocTesterUtils.getUnixRelPath(locHome, args.getStartProps());

        if (args.fastStart() != null  && consID == null)
            consID = String.format("poc-tester-%s-%s-id-%d", nodeType, host, cntr + args.fastStart());

        if (consID == null)
            consID = PocTesterUtils.findConsId(worker, host, nodeType);

        println(String.format("%s with consistent id %s on the host %s", startMsgPref, consID, host));

        String logFileName = String.format("%s/%s-%s.log", remoteLogPath, consID, dateTime);

        String envVars = String.format("LOG_FILE_NAME=%s POC_TESTER_HOME=%s", logFileName, rmtHome);

        String javaHome = args.getDefinedJavaHome() != null ? args.getDefinedJavaHome() :
            PocTesterUtils.getRemoteJavaHome(worker, host);

        if (host.equals("localhost")) {
            javaHome = args.getDefinedJavaHome() != null ? args.getDefinedJavaHome() :
                PocTesterUtils.getRemoteJavaHome(worker, host);
        }

        // If javaHome variable is defined use for launch '$JAVA_HOME/bin/java' or use simple ' java' otherwise.
        String java = (javaHome != null && !javaHome.isEmpty()) ?
            String.format(" %s/bin/java", javaHome) :
            " java";

        String nameSuffix = String.format("-%s-%s", consID, dateTime);

        String jfrOpts = null;

        if (args.getFlightRec() != null) {
            jfrOpts = PocTesterUtils.getJfrOpts(args, remoteLogPath, nameSuffix, rmtHome, null);
        }

        if (args.isJmxEnabled()) {
            String freeJmxPort = PocTesterUtils.findFreeJmxPort(worker, host);
            jmxOpts = jvmProperties.getProperty("JMX_OPTS", "");
            jmxOpts = jmxOpts.replace("-Dcom.sun.management.jmxremote.port=1101", "-Dcom.sun.management.jmxremote.port=" + freeJmxPort);
        }

        targetJvmOpts = PocTesterUtils.combineJvmOpts(targetJvmOpts, jfrOpts, jmxOpts, logFileName);

        targetJvmOpts += String.format(" -DCONSISTENT_ID=%s -DpocTesterHome=%s -DclientDirName=%s " +
                        "-DipInCluster=%s -DCACHES_CFG=%s",
            consID, rmtHome, taskDirName, host, cfgCacheRelPath.toString());

        // TODO: use %p and %t within filename
        targetJvmOpts = targetJvmOpts.replace("GC_LOG_PATH_PLACEHOLDER", String.format("%s/%s/gc%s.log", rmtHome, logDirName, nameSuffix));

        if (!worker.checkRemoteFiles(host, Arrays.asList(libPath, pocLibPath, cfg))) {
            throw new Exception();
        }

        String clsPath = String.format("-cp %s/*:%s/*", libPath, pocLibPath);

        String startPropPath = String.format("%s/%s", rmtHome, unixStartPath);

        String fullParams = fullParams(args, nodeType, startPropPath, taskDirName, cfg, cntr, total);

        return String.format("%s %s %s %s %s %s ", envVars, java, targetJvmOpts, clsPath, nodeStartCls, fullParams);
    }

    /**
     * Combines all parameters for node start in one string.
     *
     * @param args {@code PocTesterArguments} arguments.
     * @param nodeType {@code NodeType} node type (server or client).
     * @param startPropPath Path to start property file.
     * @param taskDirName Task directory name.
     * @param cfg Path to Ignite configuration file.
     * @param cntr {@code int} Unique counter which increments for each starting node.
     * @param total {@code int} Total number of nodes starting.
     * @return {@code String} Line of parameters.
     */
    private String fullParams(PocTesterArguments args, NodeType nodeType, String startPropPath, String taskDirName,
        String cfg, int cntr, int total) {
        StringBuilder sb = new StringBuilder();

        switch (nodeType) {
            case SERVER:
                // Check if WAL or WAL-archive or snapshot path are defined and add parameters for server node start.
                if (args.getWalPath() != null)
                    sb.append(String.format(" --walPath %s", args.getWalPath()));

                if (args.getWalArch() != null)
                    sb.append(String.format(" --walArch %s", args.getWalArch()));

                String servParams = String.format(" -sp %s -scfg %s ", startPropPath, cfg);

                sb.append(servParams);

                break;
            case CLIENT:
                String taskPropPath = String.format("%s/%s/%s", rmtHome, taskDirName, args.getTaskProperties());

                String clntParams = String.format(" -sp %s -tp %s -ccfg %s -cntr %d -ttl %d", startPropPath, taskPropPath,
                    cfg, cntr, total);

                sb.append(clntParams);

                break;
            default:
                LOG.error("Unknown node type");

                break;
        }

        if (args.getClientHosts() != null)
            sb.append(String.format(" --clientHosts %s", args.getClientHosts()));

        if (args.getServerHosts() != null)
            sb.append(String.format(" --serverHosts %s", args.getServerHosts()));

        return sb.toString();
    }

    public String getServerJvmOptsRandomNext() {
        if (serverJvmOptsRandom.isEmpty()) {
            // Empty random option is always present (i.e. sometimes no option is appended)
            serverJvmOptsRandom.add("");

            for (String propName : jvmProperties.stringPropertyNames())
                if (propName.contains("SERVER_JVM_OPTS_RANDOM"))
                    serverJvmOptsRandom.add(jvmProperties.getProperty(propName));

            Collections.shuffle(serverJvmOptsRandom);
        }

        return serverJvmOptsRandom.remove(0);
    }

    public String getClientJvmOptsRandomNext() {
        if (clientJvmOptsRandom.isEmpty()) {
            clientJvmOptsRandom.add("");

            for (String propName : jvmProperties.stringPropertyNames())
                if (propName.contains("CLIENT_JVM_OPTS_RANDOM"))
                    clientJvmOptsRandom.add(jvmProperties.getProperty(propName));

            Collections.shuffle(clientJvmOptsRandom);
        }

        return clientJvmOptsRandom.remove(0);
    }
}


