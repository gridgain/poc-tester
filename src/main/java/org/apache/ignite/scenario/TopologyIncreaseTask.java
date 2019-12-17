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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ignite.scenario.internal.AbstractTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.springframework.context.ApplicationContext;

import static org.apache.ignite.scenario.internal.utils.IgniteNode.loadConfiguration;

/**
 * Start several nodes with the client configuration, wait for some time, stop the nodes.
 */
public class TopologyIncreaseTask extends AbstractTask {

    private static final Logger LOG = LogManager.getLogger(TopologyIncreaseTask.class.getName());

    private final Integer threadsToRun;

    private final Long sleepBeforeStop;

    /**
     * @param args, TaskProperties props
     */
    public TopologyIncreaseTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);

        LOG.info("Topology increase task initializing");

        threadsToRun = props.getInteger("threadsToRun", 1);
        sleepBeforeStop = props.getLong("sleepBeforeStop", 0);
    }

    @Override public String getTaskReport() {
        return null;
    }

    @Override protected void body0() throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(threadsToRun);

        LOG.info("Iteration started.");

        List<Future> futs = new ArrayList<>();

        for (int i = 0; i < threadsToRun; i++) {
            futs.add(service.submit(() -> {
                try {
                    IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> tup =
                        loadConfiguration(args.getClientCfg());

                    IgniteConfiguration cfg = tup.get1()
                        .setIgniteInstanceName("IGNITE-" + UUID.randomUUID().toString());

                    try (Ignite ignite = Ignition.start(cfg)) {
                        PocTesterUtils.sleep(sleepBeforeStop);

                        LOG.debug("Current topology version {}", ignite.cluster().topologyVersion());
                    }
                    catch (Exception e) {
                        LOG.error("Failed to run ignite", e);
                    }
                }
                catch (Exception e) {
                    LOG.error("Failed to prepare ignite configuration", e);
                }
            }));
        }

        LOG.info("Pulled {} jobs to run. Waiting for result.", futs.size());

        for (Future fut : futs) {
            try {
                fut.get();
            }
            catch (Exception e) {
                LOG.error("Failed to get future result", e);
            }
        }

    }

    @Override protected boolean disableClient() {
        return true;
    }
}

