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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.cluster.ClusterNode;
import org.gridgain.poc.framework.worker.task.AbstractRestartTaskOld;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.worker.KillWorker;
import org.gridgain.poc.framework.worker.task.NodeInfo;
import org.gridgain.poc.framework.worker.NodeType;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.worker.StartNodeWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;

/**
 * Restart client nodes.
 */
public class RestartClientTask extends AbstractRestartTaskOld {
    /** */
    private static final Logger LOG = LogManager.getLogger(RestartClientTask.class.getName());

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public RestartClientTask(PocTesterArguments args) {
        super(args);
    }

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public RestartClientTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override protected List<NodeInfo> nodesToStop(int nodesToRestart) throws Exception {
        switch (mode) {
            case SEQUENTIAL:
                return seqNodes(nodesToRestart);

            case RANDOM:
                return randomNodes(nodesToRestart, NodeType.CLIENT);

            default:
                printer("Invalid mode: " + mode);

                break;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> hostSet() {
        return new HashSet<>(PocTesterUtils.getHostList(args.getClientHosts(), true));
    }

    /** {@inheritDoc} */
    @Override protected boolean checkIfRestart(NodeInfo node) {
        if(node.getNodeConsId().contains("server"))
            return false;

        for (String taskName : inclNameList)
            if (node.getStartCmd().contains(taskName))
                return true;
            else
                return false;

        for (String exclName : exclNameList)
            if (node.getStartCmd().contains(exclName))
                return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override protected int numToStop() {
        return numToRestart;
    }

    /** {@inheritDoc} */
    @Override public String reportString() {
        int nodesNum = 0;

        try {
            Collection<ClusterNode> clients = ignite().cluster().forClients().nodes();

            if (clients != null && !clients.isEmpty())
                nodesNum = clients.size();
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
        hdrMap.put("data", "number_of_client_nodes_running");
    }

    /** {@inheritDoc} */
    @Override protected Callable<NodeInfo> nodeTerminator(NodeInfo toStop){
        return new ClientNodeTerminator(toStop);
    }

    /**
     * Node terminator.
     */
    private class ClientNodeTerminator implements Callable<NodeInfo>{
        /** Node to terminate. */
        private NodeInfo toTerminate;

        /**
         * Constructor.
         * @param toTerminate {@code NodeInfo} node to terminate.
         */
        private ClientNodeTerminator(NodeInfo toTerminate){
            this.toTerminate = toTerminate;
        }

        /**
         * Terminate node.
         * @return {@code NodeInfo} node which had been terminated.
         */
        @Override public NodeInfo call() {
            new KillWorker().killOnHost(args, toTerminate.getHost(), toTerminate.getNodeConsId());

            return toTerminate;
        }
    }

    /** {@inheritDoc} */
    @Override protected Callable<NodeInfo> nodeStarter(NodeInfo toStart) {
        return new ClientNodeStarter(toStart);
    }

    /**
     * Node starter.
     */
    private class ClientNodeStarter implements Callable<NodeInfo>{
        /** Node to start. */
        private NodeInfo toStart;

        /**
         * Constructor.
         * @param toStart {@code NodeInfo} node to start.
         */
        private ClientNodeStarter(NodeInfo toStart){
            this.toStart = toStart;
        }

        /**
         * Start node.
         * @return {@code NodeInfo} node which had been started.
         */
        @Override public NodeInfo call() {
            StartNodeWorker startWorker = new StartNodeWorker();

            startWorker.setNodeType(NodeType.CLIENT);

            PocTesterArguments newArgs = args.cloneArgs();

            String oldTaskPropPath = toStart.getTaskPropertiesPath();

            int idx = oldTaskPropPath.indexOf("config/");

            String relTaskPropPath = oldTaskPropPath.substring(idx, oldTaskPropPath.length());

            LOG.info(String.format("RELPATH = %s", relTaskPropPath));

            newArgs.setTaskProperties(relTaskPropPath);

            String host = toStart.getHost();

            String consId = toStart.getNodeConsId();

            startWorker.start(newArgs, host, dateTime(), 0, 0, consId);

            return toStart;
        }
    }
}
