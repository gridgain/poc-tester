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

package org.apache.ignite.scenario.internal;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.scenario.internal.utils.NodeInfo;
import org.apache.ignite.scenario.internal.utils.WaitRebalanceTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

public abstract class AbstractRestartTask extends AbstractReportingTask {
    private static final Logger LOG = LogManager.getLogger(AbstractRestartTask.class.getName());

    private static final String DEFAULT_MODE = "RANDOM";

    private static final int DEFAULT_NUM_TO_RESTART = 1;

//    /** The list of tasks only which will be restarted. */
//    protected List<String> inclNameList = new ArrayList<>();
//
//    /** The list of tasks which should not be restarted. Effective only of inclNameList is not set. */
//    protected List<String> exclNameList = new ArrayList<>();

    /**
     * The constant DFLT_ON_CHECKPOINT_PROB.
     */
    private static final int DFLT_ON_CHECKPOINT_PROB = 50;

    private static final int DFLT_CHANGE_BLT_PROB = 0;

    /**
     * The Mode.
     */
    protected RestartMode mode;

    /**
     * The default node off-time (in seconds).
     */
    private static final int DEFAULT_OFF_TIME = 30;
    /**
     * The time while a node will be down (in milliseconds).
     */
    protected long offTime;

    /**
     * Probability of killing a node during checkpointing (in percents).
     */
    protected int onCheckpointProb;
    /**
     * The Num to restart.
     */
    protected int numToRestart;
    /**
     * Flag indicates to wait for killed nodes to start.
     */
    protected boolean waitForRebalance;

//    /** The comma-separated list of tasks which will be restarted. */
//    private String includedTaskNames;
//
//    /** The comma-separated list of tasks which should not be restarted. Effective only if includedTaskNames is not set. */
//    private String excludedTaskNames;

    /**
     * An array of possible off-times for a node (in seconds).
     */
    private int[] offTimesArray;

    /**
     * Kill mode (-2, 9, etc).
     */
    protected String killMode;

    /**
     * Flag indicates to wait for killed nodes to start.
     */
    protected boolean waitForStart;

    /**
     * Probability that baseline topology will be changed upon killing and starting a node (in percents).
     */
    private int changeBltProb;

    /**
     * A flag indicating whether to change BLT on current iteration or not.
     */
    private boolean changeBlt;

    /**
     * If true, then run idle_verify after node restart
     */
    protected boolean verifyPartitions;

    private NodeStartListener lsnr;

    public AbstractRestartTask(PocTesterArguments args) {
        super(args);
    }

    public AbstractRestartTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    @Override
    public void init() throws IOException {
        super.init();

        String offTimesStr = props.getString("offTime");
        if (offTimesStr != null)
            offTimesArray = getProbArray(offTimesStr);
        else
            offTimesArray = new int[] {DEFAULT_OFF_TIME};

        mode = RestartMode.valueOf(props.getString("mode", DEFAULT_MODE).toUpperCase());

        numToRestart = props.getInteger("numToRestart", DEFAULT_NUM_TO_RESTART);

        waitForStart = props.getBoolean("waitForStart", true);

        waitForRebalance = props.getBoolean("waitForRebalance", true);

        verifyPartitions = props.getBoolean("verifyPartitions", false);

        killMode = props.getString("killMode", "-9");

        changeBltProb = props.getInteger("changeBltProb", DFLT_CHANGE_BLT_PROB);

        onCheckpointProb = props.getInteger("onCheckpointProb", DFLT_ON_CHECKPOINT_PROB);

        if (checkKillMode()) {
            args.setKillMode(killMode);
        }
        else {
            throw new IllegalArgumentException(String.format("Illegal value for killMode property: %s", killMode));
        }
    }

    @Override
    protected void body0() throws Exception {
        if (!checkWaitFlags())
            tearDown();

        raiseSyncFlags();

        List<NodeInfo> nodesToStop = nodesToStop();

        if (nodesToStop != null) {
            Queue<NodeInfo> stoppedNodes = stopNodes(nodesToStop);

            setRandomOffTime();
            changeBlt = ifChangeBlt();

            LOG.info("Keeping killed node(s) down for {} seconds", (int) (offTime / 1000L));
            sleep(offTime);

            lsnr = new NodeStartListener(stoppedNodes);
            ignite().events().localListen(lsnr, EventType.EVT_NODE_JOINED);

            afterKill(stoppedNodes);

            // Start nodes
            startNodes(stoppedNodes);

            try {
                if (!lsnr.waitForEvent())
                    LOG.error("Failed to wait for join events for restarted nodes.");
            } catch (InterruptedException e) {
                LOG.error("Failed to wait for join events for restarted nodes.", e);
            }

            afterStart(stoppedNodes);

            clearSyncFlags();
        }
        else
            LOG.warn("Nothing to restart!");

        sleep(interval);
    }

    protected void afterKill(Queue<NodeInfo> stoppedNodes) {
        if (changeBlt) {
            try {
                resetBaseLineTopology();
            }
            catch (Exception e) {
                LOG.error("Failed to reset baseline topology", e);
            }
        }
    }

    protected void afterStart(Queue<NodeInfo> stoppedNodes) {
        // BLT change should be done before waiting for rebalance to finish.
        // Because when a node is not in baseline, then the rebalance procedure does not start on it.
        if (changeBlt) {
            try {
                resetBaseLineTopology();
            }
            catch (Exception e) {
                LOG.error("Failed to reset baseline topology", e);
            }
        }

        if (waitForRebalance)
            waitForRebalance(lsnr.nodeIds, lsnr.topVer);

        if (verifyPartitions) {
            try {
                LOG.info("Verifying consistency of primary/backup partitions");
                waitForTx();
                verifyBackupPartitions(new HashSet<>(getCacheNameList()));
            } catch (Exception e) {
                LOG.error("Verify error", e);
            }
        }
    }

    protected List<NodeInfo> nodesToStop() throws Exception {
        List<NodeInfo> availNodes = getAvailableNodes();
        int availNodesNum = availNodes.size();

        if (availNodesNum == 0) {
            LOG.warn("Found no nodes to restart");
            return null;
        }

        int toStopNum = numToRestart;
        if (toStopNum > availNodesNum) {
            toStopNum = availNodesNum;
            LOG.warn("Number of available nodes is less than number of nodes to restart. " +
                    "Will restart {} nodes.", toStopNum);
        }

        switch (mode) {
            case OLDEST:
                return getNodesOldest(availNodes, toStopNum);
            case YOUNGEST:
                return getNodesYoungest(availNodes, toStopNum);
            case RANDOM:
                return getNodesRandom(availNodes, toStopNum);
//            case SEQUENTIAL:
                // TBD
            default:
                throw new IllegalArgumentException("Invalid mode: " + mode);
        }
    }

    protected abstract List<NodeInfo> getAvailableNodes();

    protected Queue<NodeInfo> stopNodes(List<NodeInfo> nodesToStop) {
        int numToStop = nodesToStop.size();

        LOG.info("Trying to stop {} nodes.", numToStop);

        Queue<NodeInfo> res = new LinkedList<>();

        ExecutorService service = Executors.newFixedThreadPool(numToStop);

        Collection<Future<NodeInfo>> futList = new ArrayList<>(numToStop);

        for (NodeInfo node : nodesToStop)
            futList.add(service.submit(nodeTerminator(node)));

        for (Future<NodeInfo> fut : futList) {
            try {
                res.add(fut.get());
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Failed to stop nodes.", e);
            }
        }

        service.shutdown();

        return res;
    }

    /**
     *
     * @param nodeIds List of node ids.
     * @param topVer Topology version.
     */
    protected void waitForRebalance(Iterable<UUID> nodeIds, long topVer) {
        LOG.info("Wait when rebalance finish on started nodes [nodes=" + nodeIds + ", topVer=" + topVer + ']');

        long timeout = 30 * 60_000;

        Map<UUID, IgniteFuture<Boolean>> futs = new LinkedHashMap<>();

        for (UUID nodeId : nodeIds) {
            IgniteFuture<Boolean> fut =
                    ignite().compute(ignite().cluster().forNodeId(nodeId)).callAsync(new WaitRebalanceTask(topVer, timeout));

            futs.put(nodeId, fut);
        }

        boolean rebalanced = true;

        for (Map.Entry<UUID, IgniteFuture<Boolean>> futE : futs.entrySet()) {
            try {
                if (!futE.getValue().get()) {
                    LOG.error("WaitRebalanceTask failed to wait for rebalance on node: " + futE.getKey());

                    rebalanced = false;
                }
            }
            catch (Exception e) {
                LOG.error("WaitRebalanceTask failed on node: " + futE.getKey(), e);

                rebalanced = false;
            }
        }

        if (!rebalanced)
            LOG.error("Failed to wait for rebalance");
        else
            LOG.info("Finished wait for rebalance [nodes=" + nodeIds + ", topVer=" + topVer + ']');
    }

    /**
     *
     */
    private void resetBaseLineTopology() throws Exception {
        LOG.info("Resetting baseline topology");

        waitForAffinity();

        ExecutorService bltSetter = Executors.newSingleThreadExecutor();

        Future<?> f = bltSetter.submit(() -> {
            sleep(offTime / 10L);
//            ignite().cluster().setBaselineTopology(ignite().cluster().topologyVersion());
            ignite().cluster().setBaselineTopology(ignite().cluster().forServers().nodes());
        });

        f.get();

        bltSetter.shutdown();
    }

    protected void startNodes(Queue<NodeInfo> queue) {
        LOG.info(String.format("Starting killed nodes. Queue size = %d", queue.size()));

        ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        Collection<Future<?>> futList = new ArrayList<>();

        for (NodeInfo node : queue)
            futList.add(exec.submit(nodeStarter(node)));

        if (waitForStart) {
            for (Future f : futList) {
                try {
                    f.get();
                }
                catch (Exception e) {
                    LOG.error("Failed to start nodes.", e);
                }
            }
        }

        exec.shutdown();
    }

    /**
     * @return Node terminator.
     */
    protected abstract Callable<NodeInfo> nodeTerminator(NodeInfo toStop);

    /**
     * @return Node starter.
     */
    protected abstract Callable<NodeInfo> nodeStarter(NodeInfo toStart);

    /**
     * Check if value for killMode is valid.
     *
     * @return {@code true} if value is valid and {@code false} otherwise.
     */
    private boolean checkKillMode() {
        return (killMode.startsWith("-") && killMode.length() <= 3 &&
                killMode.substring(1, killMode.length()).matches("-?\\d+(\\.\\d+)?"));
    }

    protected void setRandomOffTime() {
        this.offTime = offTimesArray[COMMON_RANDOM.nextInt(offTimesArray.length)] * 1000L;
    }

    private boolean ifChangeBlt() {
        return changeBltProb > 100 * COMMON_RANDOM.nextFloat();
    }

    @Override
    protected String reportString() {
        // TBD
        return null;
    }

    /**
     * Node start listener.
     */
    static class NodeStartListener implements IgnitePredicate<Event> {
        /** */
        private final Collection<String> waitForIds = new HashSet<>();

        /** */
        private final CountDownLatch latch = new CountDownLatch(1);

        /** */
        private long topVer;

        /** */
        private Collection<UUID> nodeIds = new ArrayList<>();

        /**
         * Constructor.
         *
         * @param nodes Nodes.
         */
        NodeStartListener(Iterable<NodeInfo> nodes) {
            for (NodeInfo nodeInfo : nodes)
                waitForIds.add(nodeInfo.getNodeConsId());
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            DiscoveryEvent evt0 = (DiscoveryEvent)evt;

            ClusterNode startedNode = evt0.eventNode();

            if (waitForIds.remove(startedNode.consistentId())) {
                topVer = evt0.topologyVersion();

                LOG.info("Received expected join event [nodeId=" + startedNode.id() + ", " +
                        "nodeConsistentId=" + startedNode.consistentId() + ", " +
                        "topVer=" + topVer + ']');

                System.out.println("Received expected join event [nodeId=" + startedNode.id() + ", " +
                        "nodeConsistentId=" + startedNode.consistentId() + ", " +
                        "topVer=" + topVer + ']');

                nodeIds.add(startedNode.id());

                if (waitForIds.isEmpty()) {
                    LOG.info("Received all expected join events. topVer=" + topVer);

                    System.out.println("Received all expected join events. topVer=" + topVer);

                    latch.countDown();

                    return false;
                }
            }

            return true;
        }

        /**
         *
         * @return {@code true} if node was started if 5 minutes or {@code false} otherwise.
         * @throws InterruptedException If interrupted.
         */
        boolean waitForEvent() throws InterruptedException {
            return latch.await(5, TimeUnit.MINUTES);
        }
    }

}
