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

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LostPartChecker implements IgniteCallable<Boolean> {
    /** */
    private static final Logger LOG = LogManager.getLogger(LostPartChecker.class.getName());
    @IgniteInstanceResource
    private Ignite ignite;

    @Override public Boolean call() throws Exception {
        Collection<String> cacheNames = ignite.cacheNames();

        IgniteEx igniteEx = (IgniteEx) ignite;

        boolean lost = false;

        for(String cacheName : cacheNames) {

            int hash = igniteEx.cachex(cacheName).name().hashCode();

            CacheGroupContext cgCtx = igniteEx.context().cache().cacheGroup(hash);

            GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)cgCtx.topology();


            for (GridDhtLocalPartition local : top.localPartitions()) {
                LOG.info("State: " + local.state());

                if (local.state() == GridDhtPartitionState.LOST)
                    lost = true;
            }
        }

        return lost;
    }
}
