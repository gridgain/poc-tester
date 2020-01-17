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

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.gridgain.poc.framework.worker.task.utils.NodeStartListener;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.gridgain.poc.framework.worker.CleanWorker;
import org.gridgain.poc.framework.worker.task.NodeInfo;
import org.gridgain.poc.framework.worker.task.utils.NodeResumer;
import org.gridgain.poc.framework.worker.task.utils.NodeStopper;
import org.gridgain.poc.framework.worker.NodeType;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.sleep;

public class ResetLostPartOnRestartWithEmptyPDSTaskOld extends RestartTaskOld {
    /** */
    private static final Logger LOG = LogManager.getLogger(ResetLostPartOnRestartWithEmptyPDSTaskOld.class.getName());

    public ResetLostPartOnRestartWithEmptyPDSTaskOld(PocTesterArguments args) {
        super(args);
    }

    public ResetLostPartOnRestartWithEmptyPDSTaskOld(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }


    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        long totalSize = getTotalCachesSize();

        String threadName = Thread.currentThread().getName();

        deployServices(threadName);

        LOG.info(String.format("Total size before iteration %d", totalSize));

        List<NodeInfo> toKillList = null;

        try {
            toKillList = randomNodes(1, NodeType.SERVER);
        }
        catch (Exception e) {
            LOG.error("Failed to get list to kill.", e);
        }

        assert toKillList != null;
        assert !toKillList.isEmpty();

        NodeInfo firstToKill = toKillList.get(0);

        Queue<NodeInfo> firstStoppedNode = stopNodes(Collections.singletonList(firstToKill));

        CleanWorker cleaner = new CleanWorker(args);

        String host = firstToKill.getHost();

        String consIdDirName = firstToKill.getNodeConsId()
            .replaceAll("-", "_").replaceAll("\\.", "_");

        try {
            cleaner.cleanPds(host, consIdDirName);

            cleaner.cleanWAL(host, consIdDirName);

            cleaner.cleanWALArch(host, consIdDirName);
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to clean directories for the node %s on the host %s",
                firstToKill.getNodeConsId(), host), e);
        }

        // Waiting to make sure all files were deleted.
        sleep(20_000);

        waitForStart = false;

        NodeStartListener lsnr = new NodeStartListener(firstStoppedNode);

        ignite().events().localListen(lsnr, EventType.EVT_NODE_JOINED);

        startNodes(firstStoppedNode);

        NodeInfo startedNode = null;

        int maxCntToWaitStart = 1000;

        long startWaitTimeout = 50L;

        while(startedNode == null && maxCntToWaitStart-- > 0) {
            sleep(startWaitTimeout);

            if(maxCntToWaitStart % 200 == 0)
                LOG.info(String.format("Waiting for node %s process to launch on the host %s for another %d millis.",
                    firstToKill.getNodeConsId(), firstToKill.getHost(), maxCntToWaitStart * startWaitTimeout));

            startedNode = nodeInfo(firstToKill.getHost(), firstToKill.getNodeConsId());
        }

        final NodeInfo startedNode0 = startedNode;

        LOG.info(String.format("Trying to slow down started node %s to while there is rebalance " +
            "in progress on the started node", startedNode.getNodeConsId()));

        ExecutorService slowDownServ = Executors.newSingleThreadExecutor();

        Future<?> fut  = slowDownServ.submit(() -> {
            NodeStopper nodeStopper = new NodeStopper(args, startedNode0, "-SIGSTOP");

            NodeResumer nodeResumer = new NodeResumer(args, startedNode0, "-SIGCONT");

            for(int i = 0; i < 1000; i++){
                try {
                    nodeStopper.call();

                    sleep(80L);

                    nodeResumer.call();

                    sleep(20L);
                }
                catch (Exception e) {
                    LOG.error("Failed to stop end resume node", e);
                }
            }
        });

        try {
            waitForStringInLog(startedNode, "Started rebalance routine");
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to wait for rebalance to start on the node %s",
                startedNode.getNodeConsId()), e);
        }

        sleep(2000L);

        ignite().resetLostPartitions(ignite().cacheNames());

        try {
            fut.get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to stop end resume node", e);
        }

        slowDownServ.shutdown();

        try {
            if (!lsnr.waitForEvent())
                LOG.error("Failed to wait for join events for restarted nodes.");
        }
        catch (InterruptedException e) {
            LOG.error("Failed to wait for join events for restarted nodes.", e);
        }

        long afterResetTotalSize = getTotalCachesSize();

        int maxCheckCnt = 60;

        while(afterResetTotalSize != totalSize && maxCheckCnt-- > 0){
            if(totalSize != afterResetTotalSize)
                LOG.error(String.format("Total size after iteration and reset %s does not equal total size before %d (check %d)",
                    afterResetTotalSize, totalSize, 60 - maxCheckCnt));
            else
                LOG.info(String.format("Total size after iteration and reset %s is equal total size before %d",
                    afterResetTotalSize, totalSize));

            afterResetTotalSize = getTotalCachesSize();

            sleep(60_000L);
        }

        if(totalSize != afterResetTotalSize)
            LOG.error(String.format("Total size after iteration and reset %s does not equal total size before %d",
                afterResetTotalSize, totalSize));
        else
            LOG.info(String.format("Total size after iteration and reset %s is equal total size before %d",
                afterResetTotalSize, totalSize));

        sleep(interval);

        cancelServices(threadName);
    }

    private void checkLostPartitions(int gridNumber, String cacheName, boolean assertCheck) {
        int hash = ((IgniteEx)ignite()).cachex(cacheName).name().hashCode();

        CacheGroupContext cgCtx = ((IgniteEx)ignite()).context().cache().cacheGroup(hash);

        GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl) cgCtx.topology();

        boolean lost = false;

        for(GridDhtLocalPartition local : top.localPartitions()) {
            LOG.info("Grid: " + gridNumber + ", state: " + local.state());

            if (local.state() == GridDhtPartitionState.LOST)
                lost = true;
        }
    }

    /**
     *
     * @param nodeInfo
     * @param tgtStr Target string to be found in log file.
     * @return
     * @throws Exception
     */
    protected boolean waitForStringInLog(NodeInfo nodeInfo, String tgtStr) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String catCmd = String.format("cat %s | grep '%s'", nodeInfo.getLogPath(), tgtStr);

        List<String> response = null;

        boolean done = false;

        int cnt = 0;

        int maxCnt = 4_000;

        long timeout = 100L;

        // Replacing backslashes from target string in case we were using it for grep command above.
        tgtStr = tgtStr.replace("\\", "");

        while (!done && cnt++ < maxCnt){
            response = worker.runCmd(nodeInfo.getHost(), catCmd);

            if(response != null && !response.isEmpty() && response.get(0).contains(tgtStr)){
                LOG.info(String.format("Got '%s' string from %s log:", tgtStr, nodeInfo.getNodeConsId()));
                LOG.info(response.get(0));

                return true;
            }

            sleep(timeout);
        }

        if(!done && cnt == maxCnt) {
            LOG.error(String.format("Failed to wait for target string %s to appear in node %s log file. " +
                "Timeout (%d seconds) is exceeded", tgtStr, nodeInfo.getNodeConsId(), (maxCnt * timeout) / 1000L));

            return false;
        }

        return true;
    }
}
