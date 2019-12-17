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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.scenario.internal.AbstractRestartTaskOld;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.utils.KillWorker;
import org.apache.ignite.scenario.internal.utils.NodeInfo;
import org.apache.ignite.scenario.internal.utils.NodeType;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.ignite.scenario.internal.utils.SSHCmdWorker;
import org.apache.ignite.scenario.internal.utils.StartNodeWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Restart nodes.
 */
public class RestartTaskOld extends AbstractRestartTaskOld {
    /** */
    private static final Logger LOG = LogManager.getLogger(RestartTaskOld.class.getName());

    /** */
    private String nearHost;

    /** */
    private String nearNodeConsId;

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public RestartTaskOld(PocTesterArguments args) {
        super(args);
    }

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public RestartTaskOld(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /**
     *
     * @param numToStop Number of nodes to stop.
     * @return List of {@code NodeInfo} objects related to nodes to stop.
     */
    @Override protected List<NodeInfo> nodesToStop(int numToStop) throws Exception {
        switch (mode) {
//            case OLDEST:
//                return oldestServerNodes(numToStop);
//
//            case YOUNGEST:
//                return youngestServerNodes(numToStop);
//
            case SEQUENTIAL:
                return seqNodes(numToStop);

            case RANDOM:
                return randomNodes(numToStop, NodeType.SERVER);

            case NEAR:
                return randomNodes(numToStop, NodeType.SERVER);
            //TODO fix

            default:
                printer("Invalid mode: " + mode);

                break;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> hostSet() {
        return new HashSet<>(PocTesterUtils.getHostList(args.getServerHosts(), true));
    }

    /** {@inheritDoc} */
    @Override protected boolean checkIfRestart(NodeInfo node) {
        for (String filter : inclNameList)
            if (node.getStartCmd().contains(filter))
                return true;
            else
                return false;

        for (String filter : exclNameList)
            if (node.getStartCmd().contains(filter))
                return false;

        return node.getNodeConsId().contains("server");
    }

    /** {@inheritDoc} */
    @Override protected int numToStop() {
        int size = getServerNodes().size();

        if(size <= numToRestart) {
            LOG.warn(String.format("Number of nodes to restart %d is more or equal to actual cluster size %d. " +
                "Will restart %d nodes.", numToRestart, size, size - 1));

            return size - 1;
        }

        return numToRestart;
    }

    /**
     * @param nodeInfo Node info.
     * @throws Exception If failed.
     */
    private void waitForCheckPoint(NodeInfo nodeInfo) throws Exception {
        Random r = new Random();

        int chance = r.nextInt(100);

        if (chance + 1 > onCheckpointProb)
            return;

        LOG.info("Waiting for checkpoint.");

        SSHCmdWorker worker = new SSHCmdWorker(args);

        String grepCmd = String.format("grep -e \"Checkpoint started\" -e \"Checkpoint finished\" %s | tail -n 12",
            nodeLogPath(nodeInfo.getHost(), nodeInfo.getNodeConsId()));

        boolean checkPnt = false;

        long timeOut = 100L;

        int maxTry = 2000;

        int cnt = 0;

        while (!checkPnt && cnt++ < maxTry) {
            List<String> res = worker.runCmd(nodeInfo.getHost(), grepCmd);

            if (res.isEmpty()) {
                sleep(timeOut);

                continue;
            }

            boolean startTimeTaken = false;

            List<Long> intervals = new ArrayList<>();

            long startTime = 0;

            for (String line : res) {
                long ts = PocTesterUtils.getTimeStamp(line);

                if (!startTimeTaken && line.contains("Checkpoint started")) {
                    startTime = ts;

                    startTimeTaken = true;
                }

                if (startTimeTaken && line.contains("Checkpoint finished")) {
                    intervals.add(ts - startTime);

                    startTimeTaken = false;
                }
            }

            long total = 0L;

            for (Long interval : intervals)
                total += interval;

            long averInterval = total / intervals.size();

            for (int i = res.size() - 1; i >= 0; i--) {
                if (res.get(i).contains("Checkpoint finished"))
                    break;
                else if (res.get(i).contains("Checkpoint started")) {
                    LOG.info(String.format("Checkpoint line: %s", res.get(i)));

                    checkPnt = true;

                    LOG.info(String.format("Average checkpoint interval for last %d checkpoints is %d millis." +
                            " Will wait %d millis before killing node",
                        intervals.size(), averInterval, averInterval / 2));

                    sleep(averInterval / 2);
                }
            }
        }

        if (!checkPnt)
            LOG.error(String.format("Failed to wait for checkpoint on the node %s", nodeInfo.getNodeConsId()));

    }

    /**
     *
     * @param host Host.
     * @param nodeConsId Node consistent id.
     * @return Node log path.
     */
    private String nodeLogPath(String host, String nodeConsId) {
        SSHCmdWorker worker = new SSHCmdWorker(args);

        Map<String, String> pidMap;

        try {
            pidMap = worker.getPidMap(host, null);
        }
        catch (Exception e) {
            LOG.error(String.format("Filed to get log file path from the host %s", host), e);

            return null;
        }

        for (String key : pidMap.keySet()) {
            if (pidMap.get(key).equals(nodeConsId)) {
                List<String> argList;

                try {
                    argList = worker.runCmd(host, "ps -p " + key + " -o comm,args=ARGS");
                }
                catch (Exception e) {
                    LOG.error(String.format("Filed to get log file path from the host %s", host), e);

                    return null;
                }
                String prefix = "-DpocLogFileName=";

                for (String arg : argList) {

                    String[] argArr = arg.split(" ");

                    for (String str : argArr) {
                        if (str.startsWith(prefix))
                            return str.replace(prefix, "");

                    }
                }

            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String reportString() {
        int nodesNum = 0;

        try {
            Collection<ClusterNode> servNodes = ignite().cluster().forServers().nodes();

            if (servNodes != null && !servNodes.isEmpty())
                nodesNum = servNodes.size();
        }
        catch (Exception e) {
            LOG.error("Failed to get node number", e);
            throw e;
        }

        return String.valueOf(nodesNum);
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);
        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "number");
        hdrMap.put("data", "number_of_server_nodes_running");
    }

    /** {@inheritDoc} */
    @Override protected Callable<NodeInfo> nodeTerminator(NodeInfo toStop){
        return new ServerNodeTerminator(toStop);
    }

    /**
     * Node terminator.
     */
    private class ServerNodeTerminator implements Callable<NodeInfo>{
        /** Node to terminate. */
        private NodeInfo toTerminate;

        /**
         * Constructor.
         * @param toTerminate {@code NodeInfo} node to terminate.
         */
        private ServerNodeTerminator(NodeInfo toTerminate){
            this.toTerminate = toTerminate;
        }

        /**
         * Terminate node.
         * @return {@code NodeInfo} node which had been terminated.
         */
        @Override public NodeInfo call() {
            try {
                waitForCheckPoint(toTerminate);
            }
            catch (Exception e) {
                LOG.error(String.format("Failed to wait for checkpoint on the node %s", toTerminate.getNodeConsId()), e);
            }

            new KillWorker().killOnHost(args, toTerminate.getHost(), toTerminate.getNodeConsId());

            return toTerminate;
        }
    }

    /** {@inheritDoc} */
    @Override protected Callable<NodeInfo> nodeStarter(NodeInfo toStart) {
        return new ServerNodeStarter(toStart);
    }

    /**
     * Node starter.
     */
    private class ServerNodeStarter implements Callable<NodeInfo>{
        /** Node to start. */
        private NodeInfo toStart;

        /**
         * Constructor.
         * @param toStart {@code NodeInfo} node to start.
         */
        private ServerNodeStarter(NodeInfo toStart){
            this.toStart = toStart;
        }

        /**
         * Start node.
         * @return {@code NodeInfo} node which had been started.
         */
        @Override public NodeInfo call() {
            StartNodeWorker startWorker = new StartNodeWorker();

            startWorker.setNodeType(NodeType.SERVER);

            startWorker.start(args, toStart.getHost(), dateTime(), 0, 0, toStart.getNodeConsId());

            return toStart;
        }
    }
}
