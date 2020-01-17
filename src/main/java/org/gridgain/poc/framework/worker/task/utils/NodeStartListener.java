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

package org.gridgain.poc.framework.worker.task.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.gridgain.poc.framework.worker.task.NodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeStartListener implements IgnitePredicate<Event> {
    /** */
    private static final Logger LOG = LogManager.getLogger(NodeStartListener.class.getName());

    private final Set<String> waitForIds = new HashSet<>();

    private final CountDownLatch latch = new CountDownLatch(1);

    private long topVer;

    private List<UUID> nodeIds = new ArrayList<>();

    public NodeStartListener(Collection<NodeInfo> nodes) {
        for (NodeInfo nodeInfo : nodes)
            waitForIds.add(nodeInfo.getNodeConsId());
    }

    public long getTopVer() {
        return topVer;
    }

    public void setTopVer(long topVer) {
        this.topVer = topVer;
    }

    public List<UUID> getNodeIds() {
        return nodeIds;
    }

    public void setNodeIds(List<UUID> nodeIds) {
        this.nodeIds = nodeIds;
    }

    @Override public boolean apply(Event evt) {
        DiscoveryEvent evt0 = (DiscoveryEvent)evt;

        ClusterNode startedNode = evt0.eventNode();

        if (waitForIds.remove(startedNode.consistentId())) {
            LOG.info("Received expected join event [nodeId=" + startedNode.id() +
                ", nodeConsistentId=" + startedNode.consistentId() + ']');

            System.out.println("Received expected join event [nodeId=" + startedNode.id() +
                ", nodeConsistentId=" + startedNode.consistentId() + ']');

            topVer = evt0.topologyVersion();

            nodeIds.add(startedNode.id());

            if (waitForIds.isEmpty()) {
                LOG.info("Received all expected join events");

                System.out.println("Received all expected join events " + topVer);

                latch.countDown();

                return false;
            }
        }

        return true;
    }

    public boolean waitForEvent() throws InterruptedException {
        return latch.await(5, TimeUnit.MINUTES);
    }
}
