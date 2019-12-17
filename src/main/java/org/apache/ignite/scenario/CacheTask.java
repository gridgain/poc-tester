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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.scenario.internal.AbstractReportingTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;

import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.hms;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Creates and destroys caches.
 */
public class CacheTask extends AbstractReportingTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(CacheTask.class.getName());

    /**
     * The constant DEFAULT_MODE.
     */
    private static final String DEFAULT_MODE = "create_and_destroy";

    /**
     * The constant DEFAULT_LIFE_TIME.
     */
    private static final long DEFAULT_LIFE_TIME = 60L;

    /**
     * The Mode.
     */
    private String mode;

    /**
     * Backup count.
     */
    private int backupCount;

    /**
     * The Mode.
     */
    private String cacheMode;

    /**
     * The Mode.
     */
    private String atomicityMode;

    /**
     * The Mode.
     */
    private String writeSynchronizationMode;

    /**
     * The Off time.
     */
    private long lifeTime = DEFAULT_LIFE_TIME;

    public CacheTask(PocTesterArguments args) {
        super(args);
    }

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public CacheTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();
    }

    @Nullable @Override public String getTaskReport() throws Exception {
        return super.getTaskReport();
    }

    @Override protected String reportString() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        lifeTime = props.getLong("lifeTime", DEFAULT_LIFE_TIME) * 1000;

        mode = props.getString("mode", DEFAULT_MODE);

        cacheMode = props.getString("cacheMode", "PARTITIONED");

        atomicityMode = props.getString("atomicityMode", "TRANSACTIONAL");

        writeSynchronizationMode = props.getString("writeSynchronizationMode", "FULL_SYNC");

        backupCount = props.getInteger("backupCount", 0);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        switch (mode) {
            case "create_and_destroy":
                createCaches();

                sleep(lifeTime);

                destroyCaches();

                break;

            case "create":
                createCaches();

                break;

            case "destroy":
                destroyCaches();

                break;

            default:
                printer("Invalid mode: " + mode);

                break;
        }

        if(props.getString("timeToWork") != null)
            sleep(interval);
    }

    private void createCaches() {
        int cnt = 0;

        for (int i = cacheRangeStart; i <= cacheRangeEnd; i++) {
            String cacheName = cacheNamePrefix + i;

            if(ignite().cache(cacheName) != null)
                throw new IllegalArgumentException((String.format("Cache %s already exist.", cacheName)), null);

            LOG.info("Creating cache " + cacheName);

            IgniteCache<Object, Object> cache = ignite().getOrCreateCache(getCacheCfg(cacheName));

            try (IgniteDataStreamer<Object, Object> streamer = ignite().dataStreamer(cacheName)) {
                long total = dataRangeTo - dataRangeFrom;

                for (long i0 = dataRangeFrom; i0 < dataRangeTo; i0++) {
                    streamer.addData(i0, new SampleObject(i0, fieldCnt, fieldLen));

                    if (i0 % 1000 == 0 && i0 > dataRangeFrom) {
                        String time = hms();

                        long pcnt = ((i0 - dataRangeFrom) * 100) / total;

                        String pctStr = pcnt + "%";

                        String rep = String.format("[%s] %d keys (%s) has been added to data streamer to cache %s ",
                            time, i0, pctStr, cacheName);

                        LOG.info(rep);

                        if (i0 % 10000 == 0)
                            System.out.println(rep);
                    }
                }

                streamer.flush();

                streamer.close();

            }
            catch (Exception e) {
                LOG.error("Failed to load data", e);
            }

            cnt++;
        }

        LOG.info(String.format("%d caches created", cnt));
    }

    private void destroyCaches() {
        for (int i = cacheRangeStart; i <= cacheRangeEnd; i++) {
            String cacheName = cacheNamePrefix + i;

            LOG.info(String.format("Destroying cache %s", cacheName));

            if(ignite().cache(cacheName) != null)
                ignite().destroyCache(cacheName);
            else
                LOG.error(String.format("Failed to destroy cache %s. Cache does not exist.", cacheName));
        }
    }

    protected CacheConfiguration<Object, Object> getCacheCfg(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setBackups(backupCount);
        ccfg.setCacheMode(CacheMode.valueOf(cacheMode));
        ccfg.setAtomicityMode(CacheAtomicityMode.valueOf(atomicityMode));
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.valueOf(writeSynchronizationMode));

        return ccfg;
    }
}
