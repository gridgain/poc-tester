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

import java.util.concurrent.Callable;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeStopper implements Callable<NodeInfo> {
    /** */
    private static final Logger LOG = LogManager.getLogger(NodeStopper.class.getName());

    /** Node to terminate. */
    private NodeInfo toTerminate;

    /***/
    private PocTesterArguments args;

    /** */
    private String stopSign;

    /**
     * Constructor.
     * @param toTerminate {@code NodeInfo} node to terminate.
     */
    public NodeStopper(PocTesterArguments args, NodeInfo toTerminate, String stopSign){
        this.args = args;
        this.toTerminate = toTerminate;
        this.stopSign = stopSign;
    }

    /**
     * Terminate node.
     * @return {@code NodeInfo} node which had been terminated.
     */
    @Override public NodeInfo call() throws Exception {
        SSHCmdWorker worker = new SSHCmdWorker(args);

        String stopCmd = String.format("kill %s %s", stopSign, toTerminate.getPid());

//        LOG.info(String.format("Running cmd '%s' on the host %s", stopCmd, toTerminate.getHost()));

        worker.runCmd(toTerminate.getHost(), stopCmd);

        return toTerminate;
    }
}
