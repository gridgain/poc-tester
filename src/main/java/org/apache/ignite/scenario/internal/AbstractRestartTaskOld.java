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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.utils.LocCmdWorker;
import org.apache.ignite.scenario.internal.utils.NodeInfo;
import org.apache.ignite.scenario.internal.utils.NodeType;
import org.apache.ignite.scenario.internal.utils.SSHCmdWorker;
import org.apache.ignite.scenario.internal.utils.WaitRebalanceTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Parent for Restart tasks.
 */
public abstract class AbstractRestartTaskOld extends AbstractReportingTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(AbstractRestartTaskOld.class.getName());

    /**
     * The constant DEFAULT_MODE.
     */
    private static final String DEFAULT_MODE = "RANDOM";

    /**
     * The constant DEFAULT_NUM_TO_RESTART.
     */
    private static final int DEFAULT_NUM_TO_RESTART = 1;

    /** The list of tasks only which will be restarted. */
    protected List<String> inclNameList = new ArrayList<>();

    /** The list of tasks which should not be restarted. Effective only of inclNameList is not set. */
    protected List<String> exclNameList = new ArrayList<>();

    /**
     * The Mode.
     */
    protected RestartMode mode;

    /**
     * The time while a node will be down (in milliseconds).
     */
    protected long offTime;

    /**
     * The default node off-time (in seconds).
     */
    private static final int DEFAULT_OFF_TIME = 30;

    /**
     * An array of possible off-times for a node (in seconds).
     */
    private int[] offTimesArray;

    /**
     * The constant DFLT_ON_CHECKPOINT_PROB.
     */
    private static final int DFLT_ON_CHECKPOINT_PROB = 50;

    /**
     * The Id list.
     */
    private String idList;
    private String hostList;

    /**
     * The Num to restart.
     */
    protected int onCheckpointProb;

    /** Flag for baseline topology changing on restarts. */
    private boolean changeBltOnRestart;

    /** The comma-separated list of tasks which will be restarted. */
    private String includedTaskNames;

    /** The comma-separated list of tasks which should not be restarted. Effective only if includedTaskNames is not set. */
    private String excludedTaskNames;

    /**
     * The Num to restart.
     */
    protected int numToRestart;

    /**
     * Kill mode (-2, 9, etc).
     */
    protected String killMode;

    /**
     * Flag indicates to wait for killed nodes to start.
     */
    protected boolean waitForStart;

    /**
     * Flag indicates to wait for killed nodes to start.
     */
    protected boolean waitForRebalance;

    /**
     * If true, then run idle_verify after node restart
     */
    protected boolean verifyPartitions;

    /**
     * Flag indicates to set flags.
     */
    protected boolean checkFlag;

    private List<String> hosts;

    /** */
    private Queue<NodeInfo> seqList;

    /** */
    private String nearHost;

    /** */
    private String nearNodeConsId;

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public AbstractRestartTaskOld(PocTesterArguments args) {
        super(args);
    }

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public AbstractRestartTaskOld(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        if (mode == RestartMode.SEQUENTIAL)
            seqList = makeSeqList();

        if (includedTaskNames != null) {
            String[] taskNameArr = includedTaskNames.split(",");
            LOG.info("Only the following tasks will be restarted:");
            for (String taskName : taskNameArr) {
                LOG.info(String.format("TaskName = %s", taskName));
                inclNameList.add(taskName);
            }
        }
        else if (excludedTaskNames != null) {
            String[] taskNameArr = excludedTaskNames.split(",");
            LOG.info("The following tasks will not be restarted:");
            for (String taskName : taskNameArr) {
                LOG.info(String.format("TaskName = %s", taskName));
                exclNameList.add(taskName);
            }
        }
    }

    protected void setRandomOffTime() {
        this.offTime = offTimesArray[COMMON_RANDOM.nextInt(offTimesArray.length)] * 1000L;
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        String offTimesStr = props.getString("offTime");
        if (offTimesStr != null)
            offTimesArray = getProbArray(offTimesStr);
        else
            offTimesArray = new int[] {DEFAULT_OFF_TIME};

        idList = props.getString("idList");

        mode = RestartMode.valueOf(props.getString("mode", DEFAULT_MODE).toUpperCase());

        numToRestart = props.getInteger("numToRestart", DEFAULT_NUM_TO_RESTART);

        checkFlag = props.getBoolean("checkFlag", true);

        waitForStart = props.getBoolean("waitForStart", true);

        waitForRebalance = props.getBoolean("waitForRebalance", true);

        verifyPartitions = props.getBoolean("verifyPartitions", false);

        killMode = props.getString("killMode", "-9");

        excludedTaskNames = props.getString("excludedTaskNames");

        includedTaskNames = props.getString("includedTaskNames");

        hostList = props.getString("hostList", "");

        hosts = Arrays.asList(hostList.split(","));

        changeBltOnRestart = props.getBoolean("changeBltOnRestart", false);

        onCheckpointProb = props.getInteger("onCheckpointProb", DFLT_ON_CHECKPOINT_PROB);

        if (checkKillMode())
            args.setKillMode(killMode);
        else
            throw new IllegalArgumentException(String.format("Illegal value for killMode property: %s", killMode));
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        if (stopOnStopFlags()) {
            LOG.warn("Found raised stop flag(s). The task will stop.");
            tearDown();
        }

        if (!checkWaitFlags())
            tearDown();

        raiseSyncFlags();

        // Stop nodes
        Queue<NodeInfo> stoppedNodes = stopNodes();

        setRandomOffTime();

        LOG.info("Keeping killed node(s) down for {} seconds", (int) (offTime / 1000L));
        sleep(offTime);

        NodeStartListener lsnr = new NodeStartListener(stoppedNodes);
        ignite().events().localListen(lsnr, EventType.EVT_NODE_JOINED);

        // Start nodes
        startNodes(stoppedNodes);

        try {
            if (!lsnr.waitForEvent())
                LOG.error("Failed to wait for join events for restarted nodes.");
            else {
                if (waitForRebalance)
                    waitForRebalance(lsnr.nodeIds, lsnr.topVer);
                if (verifyPartitions) {
                    try {
                        LOG.info("Verifying consistency of primary/backup partitions");
                        waitForTx();
                        verifyBackupPartitions(new HashSet<>(getCacheNameList()));
                    }
                    catch (Exception e) {
                        LOG.error("Verify error", e);
                    }
                }
            }
        }
        catch (InterruptedException e) {
            LOG.error("Failed to wait for join events for restarted nodes.", e);
        }

        clearSyncFlags();

        sleep(interval);
    }

    /**
     * @param numToStop Number of nodes to stop.
     * @param mode Restart mode.
     * @return List of stopped nodes.
     */
    public Queue<NodeInfo> stopNodes(int numToStop, RestartMode mode) {
        this.mode = mode;

        return stopNodes();
    }

    /**
     * @return List of stopped nodes.
     */
    private Queue<NodeInfo> stopNodes() {
        int numToStop = numToStop();

        List<NodeInfo> toStopList = null;

        try {
            toStopList = nodesToStop(numToStop);
        }
        catch (Exception e) {
            LOG.error("Failed to get node list to stop", e);
        }

        return stopNodes(toStopList);
    }

    /**
     * @param toStopList List of nodes to stop.
     * @return List of stopped nodes.
     */
    protected Queue<NodeInfo> stopNodes(Iterable<NodeInfo> toStopList) {
        int numToStop = numToStop();

        LOG.info(String.format("Trying to stop %d nodes.", numToStop));

        Queue<NodeInfo> res = new LinkedList<>();

        if (mode == RestartMode.NEAR) {
            res.add(nearNodeInfo());

            return res;
        }

        ExecutorService serv = Executors.newFixedThreadPool(numToStop);

        Collection<Future<NodeInfo>> futList = new ArrayList<>(numToStop);

        for (NodeInfo toStop : toStopList)
            futList.add(serv.submit(nodeTerminator(toStop)));

        for (Future<NodeInfo> fut : futList) {
            try {
                res.add(fut.get());
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Failed to stop nodes.", e);
            }
        }

        if (changeBltOnRestart) {
            try {
                resetBaseLineTopology();
            }
            catch (Exception e) {
                LOG.error("Failed to reset baseline topology", e);
            }
        }

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
     * @return Node terminator.
     */
    protected abstract Callable<NodeInfo> nodeTerminator(NodeInfo toStop);

    /**
     * @return Node starter.
     */
    protected abstract Callable<NodeInfo> nodeStarter(NodeInfo toStart);

    /**
     * @param numToStop Number of nodes to stop.
     * @return List of stopped nodes.
     * @throws Exception If failed.
     */
    protected abstract List<NodeInfo> nodesToStop(int numToStop) throws Exception;


    protected abstract Set<String> hostSet();

    /**
     * 
     * @return Host set.
     */
    protected abstract boolean checkIfRestart(NodeInfo node);

    /**
     * 
     * @return Number of nodes to stop.
     */
    protected abstract int numToStop();

    /**
     * 
     * @return List of sequential nodes to stop. Hosts and ids of nodes should be defined in task property file.
     */
    private Queue<NodeInfo> makeSeqList() {
        seqList = new LinkedList<>();

        LOG.info("makeSeqList");

        if (idList == null) {
            LOG.error("You can't run this task in sequential mode without defining idList.");

            tearDown();
        }

        String[] consIds = idList.split(",");

        LinkedList<NodeInfo> res = new LinkedList<>();

        for (int i = 0; i < consIds.length; i++) {
            String host = hosts.get(i);

            LOG.info(String.format("Creating nodeInfo for id %s and host %s", consIds[i], host));

            String cId = String.format("poc-tester-server-%s-id-%s", host, consIds[i]);

            NodeInfo nodeInfo = nodeInfo(host, cId);

            LOG.info(String.format("Created new NodeInfo object: %s", nodeInfo));

            if (nodeInfo != null)
                res.add(nodeInfo);
            else
                LOG.error(String.format("Failed to get node info for node %s from the host %s", consIds[i], host));
        }

        if (res.isEmpty())
            LOG.error("Failed to make sequential list.");

        return res;
    }

    /**
     * 
     * @param host Host.
     * @param nodeConsId Node consistent id.
     * @return Node info related to node with the specified consistent id on the specified host.
     */
    protected NodeInfo nodeInfo(String host, String nodeConsId) {
        SSHCmdWorker worker = new SSHCmdWorker(args);

        Map<String, String> pidMap = null;

        try {
            pidMap = worker.getPidMap(host, null);
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to get node info from the host %s", host), e);

            return null;
        }

        for (String key : pidMap.keySet()) {
            LOG.info(String.format("Key = %s; val = %s", key, pidMap.get(key)));

            if (pidMap.get(key).equals(nodeConsId)) {

                List<String> argList;

                try {
                    argList = worker.runCmd(host, "ps -p " + key + " -o comm,args=ARGS");
                }
                catch (Exception e) {
                    LOG.error(String.format("Filed to get node info from the host %s", host), e);

                    return null;
                }

                String taskPropPath = null;

                String startCmd = null;

                String logPath = null;

                String logPathPrefix = "-DpocLogFileName=";

                for (String arg : argList) {
                    if (arg.startsWith("java"))
                        startCmd = arg;

                    String[] argArr = arg.split(" ");

                    for (int i = 0; i < argArr.length; i++) {
                        String currStr = argArr[i];

                        if ((currStr.equals("-tp") || currStr.equals("--taskProperties")) && i < argArr.length - 1)
                            taskPropPath = argArr[i + 1];

                        if (currStr.startsWith(logPathPrefix))
                            logPath = currStr.replace(logPathPrefix, "");
                    }
                }

                return new NodeInfo(args, host, nodeConsId, key, taskPropPath, startCmd, logPath);
            }
        }

        return null;
    }

    /**
     * 
     * @param numToStop Number of nodes to stop.
     * @return List of nodes to stop.
     */
    protected List<NodeInfo> seqNodes(int numToStop) {
        List<NodeInfo> res = new ArrayList<>(numToStop);

        for (int i = 0; i < numToStop; i++) {
            if (seqList.isEmpty())
                seqList = makeSeqList();

            NodeInfo toKill = seqList.poll();

            res.add(toKill);
        }

        return res;
    }

    protected List<NodeInfo> randomNodes(int numToStop) throws Exception {
        return randomNodes(numToStop, null);
    }

    /**
     *
     * @param numToStop Number of nodes to stop.
     * @return List of nodes to stop.
     * @throws Exception If failed.
     */
    protected List<NodeInfo> randomNodes(int numToStop, NodeType nodeType) throws Exception {
        LinkedList<NodeInfo> middleRes = new LinkedList<>();

        Set<String> hosts = hostSet();

        SSHCmdWorker worker = new SSHCmdWorker(args);

        for (String host : hosts) {
            Map<String, String> pidMap = worker.getPidMap(host, nodeType);

            for (String consId : pidMap.values()) {
                NodeInfo nodeInfo = nodeInfo(host, consId);

                if (nodeInfo != null && checkIfRestart(nodeInfo))
                    middleRes.add(nodeInfo);
            }
        }

        int middleResSize = middleRes.size();

        if (middleResSize < numToStop) {
            LOG.error(String.format("Found %d possible nodes to restart instead of %d, will restart %d nodes.",
                middleResSize, numToStop, middleResSize));

            numToStop = middleResSize;
        }

        Collections.shuffle(middleRes);

        List<NodeInfo> res = new ArrayList<>();

        for (int i = 0; i < numToStop; i++)
            res.add(middleRes.poll());

        return res;
    }

    /**
     *
     * @param queue Nodes to start.
     */
    public void startNodes(Collection<NodeInfo> queue) {
        LOG.info(String.format("Starting killed nodes. Queue size = %d", queue.size()));

        boolean isDataLost = false;

        if (checkFlag) {
            try {
                isDataLost = isDataLost();
            }
            catch (Exception e) {
                LOG.error("Failed to check dataLost flag. Will not start killed nodes.", e);

                return;
            }
        }

        if (isDataLost) {
            LOG.info("Found data lost flag. Will not start killed nodes.");

            return;
        }

        ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        Collection<Future<?>> futList = new ArrayList<>();

        for (NodeInfo toStart : queue)
            futList.add(exec.submit(nodeStarter(toStart)));

        if (waitForStart) {
            for (Future f : futList) {
                try {
                    f.get();
                }
                catch (Exception e) {
                    LOG.error("Failed to start nodes", e);
                }
            }
        }

        exec.shutdown();

        if (changeBltOnRestart) {
            try {
                resetBaseLineTopology();
            }
            catch (Exception e) {
                LOG.error("Failed to reset baseline topology", e);
            }
        }
    }

    /**
     *
     */
    private void resetBaseLineTopology() throws Exception {
        waitForAffinity();

        ExecutorService bltSetter = Executors.newSingleThreadExecutor();

        Future<?> f = bltSetter.submit(new Runnable() {
            @Override public void run() {
                sleep(offTime / 10L);

                ignite().cluster().setBaselineTopology(ignite().cluster().topologyVersion());
            }
        });

        f.get();

        bltSetter.shutdown();
    }

    /**
     *
     * @return Node info.
     */
    private NodeInfo nearNodeInfo() {
        LOG.info("kill near");

        LocCmdWorker worker = new LocCmdWorker(args);

        try {
            List<String> processes = worker.runCmd(null, "ps -ax");

            String proc = null;

            for (String pr : processes)
                if (pr.contains("-DCONSISTENT_ID=poc-tester-server"))
                    proc = pr;

            if (proc != null) {
                String[] arr = proc.split(" ");

                Integer procNum = null;

                for (String str : arr) {
                    if (!str.isEmpty() && procNum == null)
                        procNum = Integer.valueOf(str);

                    if ((nearHost == null || nearNodeConsId == null) && str.startsWith("-DCONSISTENT_ID=poc-tester-server-")) {
                        nearNodeConsId = str.substring(16, str.length());

                        int idIdx = nearNodeConsId.indexOf("-id-");

                        nearHost = nearNodeConsId.substring(18, idIdx);

                        LOG.info("procNum=" + procNum);
                        LOG.info("nearNodeConsId=" + nearNodeConsId);
                        LOG.info("idIdx=" + idIdx);
                        LOG.info("nearHost=" + nearHost);
                    }

                }

                if (procNum != null)
                    worker.runCmd(null, String.format("kill -9 %d", procNum));
            }

        }
        catch (Exception e) {
            LOG.error("Failed to get near node info", e);
        }

        return new NodeInfo(nearHost, nearNodeConsId, args);
    }



    private String getNodeLogPath(String host, String nodeConsId) {
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
                String prefix = "-DpocLogFileName=";

                List<String> argList = null;
                try {
                    argList = worker.runCmd(host, "ps -p " + key + " -o comm,args=ARGS");
                }
                catch (Exception e) {
                    LOG.error(String.format("Filed to get log file path from the host %s", host));

                    return null;
                }

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
            Collection<ClusterNode> nodes = ignite().cluster().forServers().nodes();

            if (nodes != null && !nodes.isEmpty())
                nodesNum = nodes.size();
        }
        catch (Exception e) {
            LOG.error("Failed to get node number", e);
        }

        return String.valueOf(nodesNum);
    }


    /**
     * Check if value for killMode is valid.
     *
     * @return {@code true} if value is valid and {@code false} otherwise.
     */
    private boolean checkKillMode() {
        return (killMode.startsWith("-") && killMode.length() <= 3 &&
            killMode.substring(1, killMode.length()).matches("-?\\d+(\\.\\d+)?"));
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);
        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "number");
        hdrMap.put("data", "number_of_server_nodes_running");
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
