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

import java.io.IOException;
import java.util.ArrayList;
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
import org.gridgain.poc.framework.worker.task.NodeInfo;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;

/**
 * Stop and resume processes without terminating.
 */
public class StopResumeTaskOld extends AbstractRestartTaskOld {
    /** */
    private static final Logger LOG = LogManager.getLogger(StopResumeTaskOld.class.getName());

    /** */
    private static final String DFLT_NODETYPES = "client,server";

    /** */
    private static final String DFLT_STOP_SIGN = "-SIGSTOP";

    /** */
    private static final String DFLT_RESUME_SIGN = "-SIGCONT";

    /** */
    private String stopSign;

    /** */
    private String resumeSign;

    /** */
    private String nodeTypes;

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public StopResumeTaskOld(PocTesterArguments args) {
        super(args);
    }

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public StopResumeTaskOld(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }



    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        killMode = props.getString("killMode", "-9");

        nodeTypes = props.getString("nodeTypes", DFLT_NODETYPES);

        stopSign = props.getString("stopSign", DFLT_STOP_SIGN);

        resumeSign = props.getString("resumeSign", DFLT_RESUME_SIGN);
    }

    /** {@inheritDoc} */
    @Override protected List<NodeInfo> nodesToStop(int numToStop) throws Exception {
        switch (mode) {
//            case OLDEST:
//                return oldestServerNodes(numToStop);

            case SEQUENTIAL:
                return seqNodes(numToStop);

            case RANDOM:
                return randomNodes(numToStop, null);

            default:
                printer("Invalid mode: " + mode);

                break;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> hostSet() {
        List<String> hosts = new ArrayList<>();

        if(nodeTypes.contains("server"))
            hosts.addAll(PocTesterUtils.getHostList(args.getServerHosts(), true));

        if(nodeTypes.contains("client"))
            hosts.addAll(PocTesterUtils.getHostList(args.getClientHosts(), true));

        return new HashSet<>(hosts);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkIfRestart(NodeInfo node) {
        for(String exclName : exclNameList)
            if(node.getStartCmd().contains(exclName))
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
            Collection<ClusterNode> nodes = ignite().cluster().forServers().nodes();

            if (nodes != null && !nodes.isEmpty())
                nodesNum = nodes.size();
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
        return new NodeTerminator(toStop);
    }

    /**
     * Node terminator.
     */
    private class NodeTerminator implements Callable<NodeInfo>{
        /** Node to terminate. */
        private NodeInfo toTerminate;

        /**
         * Constructor.
         * @param toTerminate {@code NodeInfo} node to terminate.
         */
        private NodeTerminator(NodeInfo toTerminate){
            this.toTerminate = toTerminate;
        }

        /**
         * Terminate node.
         * @return {@code NodeInfo} node which had been terminated.
         */
        @Override public NodeInfo call() throws Exception {
            RemoteSshExecutor worker = new RemoteSshExecutor(args);

            String stopCmd = String.format("kill %s %s", stopSign, toTerminate.getPid());

            LOG.info(String.format("Running kill cmd '%s' on the host %s", stopCmd, toTerminate.getHost()));

            worker.runCmd(toTerminate.getHost(), stopCmd);

            return toTerminate;
        }
    }

    /** {@inheritDoc} */
    @Override protected Callable<NodeInfo> nodeStarter(NodeInfo toStart) {
        return new NodeResumer(toStart);
    }

    /**
     * Node starter.
     */
    private class NodeResumer implements Callable<NodeInfo>{
        /** Node to start. */
        private NodeInfo toResume;

        /**
         * Constructor.
         * @param toResume {@code NodeInfo} node to start.
         */
        private NodeResumer(NodeInfo toResume){
            this.toResume = toResume;
        }

        /**
         * Start node.
         * @return {@code NodeInfo} node which had been started.
         */
        @Override public NodeInfo call() throws Exception {
            RemoteSshExecutor worker = new RemoteSshExecutor(args);

            String resumeCmd = String.format("kill %s %s", resumeSign, toResume.getPid());

            worker.runCmd(toResume.getHost(), resumeCmd);

            return null;
        }
    }
}
