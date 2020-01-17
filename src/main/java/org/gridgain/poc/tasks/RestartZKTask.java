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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import org.gridgain.poc.framework.worker.task.AbstractRestartTaskOld;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.gridgain.poc.framework.worker.KillWorker;
import org.gridgain.poc.framework.worker.task.NodeInfo;
import org.gridgain.poc.framework.worker.NodeType;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.StartZKWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;
import static org.gridgain.poc.framework.utils.PocTesterUtils.sleep;

public class RestartZKTask extends AbstractRestartTaskOld {
    private static final Logger LOG = LogManager.getLogger(RestartZKTask.class.getName());

    public RestartZKTask(PocTesterArguments args) {
        super(args);
    }

    public RestartZKTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        Queue<NodeInfo> stoppedNodesInfo = stopNodes(numToStop(), mode);
        sleep(offTime);
        startNodes(stoppedNodesInfo);
        sleep(interval);
    }

    @Override protected List<NodeInfo> nodesToStop(int nodesToRestart) throws Exception {
        switch (mode) {
            case RANDOM:
                return randomNodes(nodesToRestart, NodeType.ZOOKEEPER);
            default:
                LOG.error("Invalid mode: " + mode);
                throw new TestFailedException();
        }
    }

    @Override protected List<NodeInfo> randomNodes(int numToStop, NodeType nodeType) throws Exception {
        LinkedList<NodeInfo> availZkNodes = new LinkedList<>();
        List<NodeInfo> zkNodes = new ArrayList<>();

        Set<String> zkHosts = hostSet();
        RemoteSshExecutor sshWorker = new RemoteSshExecutor(args);

        for (String host : zkHosts) {
            Map<String, String> zkPidMap = sshWorker.getPidMap(host, nodeType);
            for (String pid : zkPidMap.keySet()) {
                availZkNodes.add(new NodeInfo(host, zkPidMap.get(pid), pid, args));
            }
        }
        Collections.shuffle(availZkNodes);

        int zkNodesCnt = availZkNodes.size();
        if (zkNodesCnt < numToStop) {
            LOG.warn("Number of available nodes is less than number of nodes to restart. Will adjust the latter.");
            numToStop = zkNodesCnt;
        }

        for (int i = 0; i < numToStop; i++)
            zkNodes.add(availZkNodes.poll());

        return zkNodes;
    }

        /** {@inheritDoc} */
    @Override protected Set<String> hostSet() {
        return new HashSet<>(PocTesterUtils.getZooKeeperHosts(args, true));
    }

    /** {@inheritDoc} */
    @Override protected boolean checkIfRestart(NodeInfo node) {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected int numToStop() {
        return numToRestart;
    }

    @Override public String reportString() {
        int nodesNum = 0;

        Set<String> zkHosts = hostSet();
        RemoteSshExecutor sshWorker = new RemoteSshExecutor(args);

        try {
            for (String host : zkHosts) {
                Map<String, String> zkPidMap = sshWorker.getPidMap(host, NodeType.ZOOKEEPER);
                nodesNum += zkPidMap.keySet().size();
            }
        }
        catch (Exception e) {
            LOG.error("Failed to get PIDs for ZooKeeper hosts.");
        }

        return String.valueOf(nodesNum);
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);
        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "number");
        hdrMap.put("data", "number_of_zk_nodes_running");
    }

    /** {@inheritDoc} */
    @Override protected Callable<NodeInfo> nodeTerminator(NodeInfo toStop){
        return new ZkNodeTerminator(toStop);
    }

    private class ZkNodeTerminator implements Callable<NodeInfo>{
        private NodeInfo toTerminate;

        private ZkNodeTerminator(NodeInfo toTerminate){
            this.toTerminate = toTerminate;
        }

        @Override public NodeInfo call() {
            new KillWorker().killOnHostByPid(args, toTerminate.getHost(), toTerminate.getPid());
            return toTerminate;
        }
    }


    /** {@inheritDoc} */
    @Override protected Callable<NodeInfo> nodeStarter(NodeInfo toStart) {
        return new ZkNodeStarter(toStart);
    }

    private class ZkNodeStarter implements Callable<NodeInfo>{
        private NodeInfo toStart;

        private ZkNodeStarter(NodeInfo toStart){
            this.toStart = toStart;
        }

        @Override public NodeInfo call() {
            StartZKWorker startWorker = new StartZKWorker();
            String host = toStart.getHost();
            int zkNodeId = Integer.parseInt(toStart.getNodeConsId());
            startWorker.start(args, host, dateTime(), zkNodeId, 0, null);

            return toStart;
        }
    }
}
