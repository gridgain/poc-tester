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

package org.gridgain.poc.framework.worker.task;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_PID;

/**
 * Node info class.
 */
public class NodeInfo {
    /** */
    private static final Logger LOG = LogManager.getLogger(NodeInfo.class.getName());

    /** */
    private PocTesterArguments args;

    /**
     * The Host.
     */
    protected String host;
    /**
     * The Node cons id.
     */
    protected String nodeConsId;

    protected String pid;

    protected long order;

    protected String taskPropertiesPath;

    protected String startCmd;

    private String logPath;

    public NodeInfo(ClusterNode node) {
        this.host = node.attribute("ipInCluster");
        this.logPath = node.attribute("pocLogFileName");
        this.nodeConsId = node.attribute("CONSISTENT_ID");
        this.pid = node.attribute(ATTR_JVM_PID).toString();
        this.order = node.order();
        this.startCmd = node.attribute("sun.java.command");

        String[] argArray = startCmd.split(" +");

        // Remove Java class from command-line
        argArray = Arrays.copyOfRange(argArray, 1, argArray.length);

        args = new PocTesterArguments();

        // Parse command-line
        args.setArgsFromCmdLine(argArray);

        // Get missing parameters from common properties
        args.setArgsFromStartProps();
    }

    // TODO: remove
    public NodeInfo(String host, String nodeConsId, PocTesterArguments args) {
        this.args = args;
        this.host = host;
        this.nodeConsId = nodeConsId;
    }

    public NodeInfo(String host, String nodeConsId, String pid, PocTesterArguments args) {
        this.args = args;
        this.host = host;
        this.pid = pid;
        this.nodeConsId = nodeConsId;
    }

    // TODO: remove
    public NodeInfo(PocTesterArguments args, String host, String nodeConsId, String pid,
                    String taskPropertiesPath, String startCmd, String logPath) {
        this.args = args;
        this.host = host;
        this.nodeConsId = nodeConsId;
        this.pid = pid;
        this.taskPropertiesPath = taskPropertiesPath;
        this.startCmd = startCmd;
        this.logPath = logPath;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getNodeConsId() {
        return nodeConsId;
    }

    public void setNodeConsId(String nodeConsId) {
        this.nodeConsId = nodeConsId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public long getOrder() {
        return order;
    }

    public void setOrder(long order) {
        this.order = order;
    }

    public String getTaskPropertiesPath() {
        return taskPropertiesPath;
    }

    public void setTaskPropertiesPath(String taskPropertiesPath) {
        this.taskPropertiesPath = taskPropertiesPath;
    }

    public PocTesterArguments getArgs() {
        return args;
    }

    public void setArgs(PocTesterArguments args) {
        this.args = args;
    }

    public String getStartCmd() {
        return startCmd;
    }

    public void setStartCmd(String startCmd) {
        this.startCmd = startCmd;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }

    /** {@inheritDoc} */
    public boolean theSame(Object obj) {
        NodeInfo another = (NodeInfo)obj;

        return this.nodeConsId.equals(another.nodeConsId) && this.host.equals(another.host);
    }

    @Override public String toString() {
        return "NodeInfo{" +
            "host='" + host + '\'' +
            ", nodeConsId='" + nodeConsId + '\'' +
            ", pid='" + pid + '\'' +
            ", taskPropertiesPath='" + taskPropertiesPath + '\'' +
            ", args=" + args +
            '}';
    }
}
