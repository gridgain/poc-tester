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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.scenario.internal.AbstractReportingTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.ignite.scenario.internal.utils.SSHCmdWorker;
import org.apache.ignite.scenario.internal.utils.checkers.FileCleanerClosure;
import org.apache.ignite.scenario.internal.utils.checkers.HeapChecker;
import org.apache.ignite.scenario.internal.utils.checkers.HeapSizeCheckType;
import org.apache.ignite.scenario.internal.utils.checkers.NodeHeapInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Checks heap size.
 */
public class CheckHeapSizeTask extends AbstractReportingTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(CheckHeapSizeTask.class.getName());

    /** */
    private static final long MB = 1024 * 1024;

    /** */
    private HeapSizeCheckType checkType;

    /** */
    private static final HeapSizeCheckType DEFAULT_HEAP_CHECK_TYPE = HeapSizeCheckType.JMX;

    /** */
    private Map<UUID, List<NodeHeapInfo>> nodeHeapMap = new HashMap<>();

    /** */
    private Float coefficient;

    /** */
    private int failuresBeforeStop;

    /** */
    private int failCnt;

    /** */
    private Path heapDumpDir;

    /** Number of oldest nodes in cluster for which heap dumps will be created */
    private int nodesToCheck;

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public CheckHeapSizeTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        checkType = HeapSizeCheckType.valueOf(props.getString("checkType", DEFAULT_HEAP_CHECK_TYPE.toString()));
        coefficient = props.getFloat("coefficient", 1.5F);
        failuresBeforeStop = props.getInteger("failuresBeforeStop", 1);
        nodesToCheck = props.getInteger("nodesToCheck", 1);
    }

    @Override public void setUp() throws Exception {
        super.setUp();

//        heapDumpDir = Paths.get(homeDir, "heap-dumps", clientDirName);
        heapDumpDir = Paths.get(homeDir, "log", "server", "heap-dumps");
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        List<ClusterNode> srvNodes = getServerNodesSorted(true);

        if (srvNodes.size() < nodesToCheck) {
            LOG.warn("Cluster has less nodes than the value of 'nodesToCheck'. Adjusting 'nodesToCheck'.");
            nodesToCheck = srvNodes.size();
        }
        LOG.info("Will check heap for {} oldest nodes. Check type: {}", nodesToCheck, checkType);

        for (int i = 0; i < nodesToCheck; i++) {
            ClusterNode node = srvNodes.get(i);
            UUID nodeId = node.id();

            String nodeHost = node.attributes().get("ipInCluster").toString();

            String nodeConsId = node.consistentId().toString();

            ClusterGroup grp = ignite().cluster().forNodeId(nodeId);

            IgniteCompute compute = ignite().compute(grp);

            String dumpPath = Paths.get(heapDumpDir.toString(), String.format("%s.hprof", dateTime()))
                .toString();

            long ts = System.currentTimeMillis();

            LOG.info("Starting heap checker on node {}", nodeConsId);
            long res = compute.apply(new HeapChecker(args, checkType), dumpPath);

            List<NodeHeapInfo> heapInfoList = nodeHeapMap.get(nodeId);

            if (heapInfoList == null) {
                heapInfoList = new ArrayList<>();

                NodeHeapInfo nodeHeapInfo = new NodeHeapInfo(ts, res);

                heapInfoList.add(nodeHeapInfo);

                nodeHeapMap.put(nodeId, heapInfoList);

                compute.apply(new FileCleanerClosure(), dumpPath);
            }
            else {
                long firstSize = heapInfoList.get(0).heapSize();

                LOG.info(String.format("Heap size on node %s:", nodeConsId));
                LOG.info(String.format(" - First: %d MiB", firstSize/MB));
                LOG.info(String.format(" - Last:  %d MiB", res/MB));
                LOG.info(String.format(" - Diff:  %.2f%%", 100 * (float)(res - firstSize) / firstSize));

                LOG.debug(String.format("Heap size history for %s:", nodeConsId));
                for (NodeHeapInfo info : heapInfoList)
                    LOG.debug(String.format("Time = %s; Size = %d MiB", PocTesterUtils.dateTime(info.timeStamp()),
                            info.heapSize() / MB));

                if (res > firstSize * coefficient) {
                    LOG.warn(String.format("Current heap size %d MiB on the node '%s' is %.2f times bigger " +
                            "than first heap size %d MiB",
                        res / MB, nodeConsId,
                        (float)res / (float)firstSize,
                        firstSize / MB));

                    if (checkType == HeapSizeCheckType.JMX) {
                        // Create a heap dump for analysis
                        LOG.info("Creating a heap dump on node {}", nodeConsId);
                        compute.apply(new HeapChecker(args, HeapSizeCheckType.FILE_SIZE), dumpPath);
                    }


                    SSHCmdWorker sshCmdWorker = new SSHCmdWorker(args);

                    try {
                        // Compress heap dump
                        LOG.info("Compressing the heap dump '{}' on node {}", dumpPath, nodeConsId);
                        sshCmdWorker.compressFile(nodeHost, dumpPath);

                        LOG.info("Deleting the heap dump '{}' on node {}", dumpPath, nodeConsId);
                        sshCmdWorker.removeFile(nodeHost, dumpPath);
                    }
                    catch (Exception e) {
                        LOG.error("Failed to compress and/or remove heap dump {} on host {}", dumpPath, nodeHost);
                    }

                    failCnt++;

                    if(failCnt >= failuresBeforeStop) {
                        LOG.info("Heap size list content before failing the task:");

                        for (NodeHeapInfo info : heapInfoList)
                            LOG.info(String.format("Time = %s; Size = %d MiB", PocTesterUtils.dateTime(info.timeStamp())
                                , info.heapSize() / MB));

                        throw new TestFailedException();
                    }
                }
                else
                    compute.apply(new FileCleanerClosure(), dumpPath);

                heapInfoList.add(new NodeHeapInfo(ts, res));
            }
        }

        sleep(interval);
    }

    /** {@inheritDoc} */
    @Override protected String reportString() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable public String getTaskReport() {
        //TODO: avoid null result.
        return null;
    }

    /** */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>)propMap.get("headersMap");

        hdrMap.put("unit", "boolean");
        hdrMap.put("data", "status");

        propMap.put("reportDir", "reports");
    }
}
