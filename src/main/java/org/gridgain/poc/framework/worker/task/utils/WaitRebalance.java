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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.gridgain.poc.tasks.CheckBroadcastTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WaitRebalance implements IgniteCallable<Boolean> {
    /** */
    private static final Logger LOG = LogManager.getLogger(WaitRebalance.class.getName());

    @IgniteInstanceResource
    private Ignite ignite;

    private final long topVer;

    private final long timeout;

    public WaitRebalance(long topVer, long timeout) {
        this.topVer = topVer;
        this.timeout = timeout;
    }

    @Override public Boolean call() throws Exception {
        long endTime = System.currentTimeMillis() + timeout;

        boolean rebalanced;

        do {
            rebalanced = true;

            AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer);

            for (CacheGroupContext ctx : ((IgniteKernal)ignite).context().cache().cacheGroups()) {
                if (ctx.isLocal())
                    continue;

                if (!ctx.topology().initialized()) {
                    LOG.info("Ready topVer is not initialized yet [grp=" + ctx.cacheOrGroupName() + "]");
                    rebalanced = false;
                    break;
                }

                AffinityTopologyVersion readyVer = ctx.topology().readyTopologyVersion();

                if (topVer0.compareTo(readyVer) > 0) {
                    LOG.info("Wait for ready topVer [grp=" + ctx.cacheOrGroupName() + ", topVer=" + readyVer + "]");
                    rebalanced = false;
                    break;
                }

                if (!ctx.topology().rebalanceFinished(readyVer)) {
                    LOG.info("Wait for rebalance [grp=" + ctx.cacheOrGroupName() + ", topVer=" + readyVer + "]");
                    rebalanced = false;
                    break;
                }
            }

            if (rebalanced)
                break;

            Thread.sleep(10_000);
        } while (System.currentTimeMillis() < endTime);


        return rebalanced;
    }
}
