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

import org.apache.ignite.cluster.ClusterNode;
import org.gridgain.poc.framework.worker.task.AbstractRestartTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.worker.KillWorker;
import org.gridgain.poc.framework.worker.task.NodeInfo;
import org.gridgain.poc.framework.worker.NodeType;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.StartNodeWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;
import static org.gridgain.poc.framework.utils.PocTesterUtils.sleep;

public class RestartServerTask extends AbstractRestartTask {
    private static final Logger LOG = LogManager.getLogger(RestartServerTask.class.getName());

    public RestartServerTask(PocTesterArguments args) {
        super(args);
    }

    public RestartServerTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    @Override
    protected List<NodeInfo> getAvailableNodes() {
        List<NodeInfo> availNodesInfo = new LinkedList<>();

        LOG.info("Getting server nodes info");

        Collection<ClusterNode> availNodes = getServerNodes();

        LOG.info("Found {} node(s)", availNodes.size());

        for (ClusterNode node : availNodes) {
            NodeInfo nodeInfo = new NodeInfo(node);

            availNodesInfo.add(nodeInfo);
        }

        return availNodesInfo;
    }

    /**
     * @param nodeInfo Node info.
     * @throws Exception If failed.
     */
    private void waitForCheckPoint(NodeInfo nodeInfo) throws Exception {
//        Random r = new Random();

        int chance = COMMON_RANDOM.nextInt(100);

        if (chance + 1 > onCheckpointProb)
            return;

        LOG.info("Waiting for checkpoint.");

        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String grepCmd = String.format("grep -e \"Checkpoint started\" -e \"Checkpoint finished\" %s | tail -n 12",
                nodeInfo.getLogPath());

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

            if (intervals.isEmpty()) {
                sleep(timeOut);
                continue;
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

    @Override
    protected String reportString() {
        int nodesNum = getServerNodes().size();

        return String.valueOf(nodesNum);
    }

    @Override
    protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "number");
        hdrMap.put("data", "number_of_server_nodes_running");
    }

    @Override
    protected Callable<NodeInfo> nodeTerminator(NodeInfo toStop) {
        return new ServerNodeTerminator(toStop);
    }

    @Override
    protected Callable<NodeInfo> nodeStarter(NodeInfo toStart) {
        return new ServerNodeStarter(toStart);
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

//            new KillWorker().killOnHost(args, toTerminate.getHost(), toTerminate.getNodeConsId());
            new KillWorker().killOnHostByPid(args, toTerminate.getHost(), toTerminate.getPid());

            return toTerminate;
        }
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

            startWorker.start(toStart.getArgs(), toStart.getHost(), dateTime(), 0, 0, toStart.getNodeConsId());

            return toStart;
        }
    }

}
