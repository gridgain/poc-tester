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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.gridgain.poc.framework.worker.task.AbstractTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates and destroys simple cache.
 */
public class CreateDestroyCacheTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(CreateDestroyCacheTask.class.getName());

    private static final String CACHE_NAME = "dynamic-cache-";

    /** A counter to make cache names unique. */
    private static final AtomicInteger cntr = new AtomicInteger();

    /** Keys to put into cache before its destruction. */
    private long keysToPut;

    private int backups;

    private String atomicityMode;

    private String cacheMode;

    private String writeSynchronizationMode;

    /** Name of a cache group where caches will be created. */
    private String cacheGroup;

    private String partLossPolicy;

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public CreateDestroyCacheTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    @Override
    public void init() throws IOException {
        super.init();

        keysToPut = props.getLong("keysToPut", 0);

        backups = props.getInteger("backups", 0);
        atomicityMode = props.getString("atomicityMode", "TRANSACTIONAL");
        cacheMode = props.getString("cacheMode", "PARTITIONED");
        writeSynchronizationMode = props.getString("writeSynchronizationMode", "FULL_SYNC");
        cacheGroup = props.getString("cacheGroup");
        partLossPolicy = props.getString("partLossPolicy", "READ_WRITE_SAFE");
    }

    /** {@inheritDoc} */
    @Override
    public void body0() throws TestFailedException {
        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME + cntr.getAndIncrement());

        ccfg.setBackups(backups)
                .setAtomicityMode(CacheAtomicityMode.valueOf(atomicityMode))
                .setCacheMode(CacheMode.valueOf(cacheMode))
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.valueOf(writeSynchronizationMode))
                .setPartitionLossPolicy(PartitionLossPolicy.valueOf(partLossPolicy));

        if (cacheGroup != null)
            ccfg.setGroupName(cacheGroup);

        try {
            IgniteCache cache = ignite().createCache(ccfg);

            LOG.debug("Cache created: " + cache.getName());

            if (keysToPut > 0) {
                LOG.debug("Populating cache " + cache.getName());
                for (long k = 0; k < keysToPut; k++)
                    cache.put(k, "Value" + k);
            }

            cache.destroy();

            LOG.debug("Cache destroyed: " + cache.getName());
        }
        catch (Exception e) {
            LOG.error(String.format(e.getMessage()), e);
        }
    }
}
