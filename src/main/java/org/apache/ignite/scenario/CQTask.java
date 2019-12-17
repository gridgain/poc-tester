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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.utils.checkers.HeapSizeCheckType;
import org.apache.ignite.scenario.internal.utils.checkers.NodeHeapInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;

/**
 * Performs continuous query.
 */
public class CQTask extends CacheTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(CQTask.class.getName());

    /** */
    private IgniteCache<Object, Object> cache;

    /** */
    private boolean setRemoteFilterFactory;

    /** */
    private boolean setLocalQuery;

    /** */
    private String cacheNameForContQuery;

    /** */
    private HeapSizeCheckType checkType;

    /** */
    private int registeredQryCnt = 1;

    /** */
    private Map<UUID, List<NodeHeapInfo>> nodeHeapMap = new HashMap<>();

    /** */
    private long exitSetUpTime;

    /** */
    private List<ContinuousQuery<Object, Object>> qryList = new ArrayList<>();

    /** */
    private List<QueryCursor<Cache.Entry<Object, Object>>> cursorList = new ArrayList<>();

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public CQTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        registeredQryCnt = props.getInteger("registeredQryCnt", 1);

        setRemoteFilterFactory = props.getBoolean("setRemoteFilterFactory", false);

        setLocalQuery = props.getBoolean("setLocalQuery", false);

        cacheNameForContQuery = props.getString("cacheNameForContQuery", null);
    }

    @Override public void setUp() throws Exception {
        super.setUp();

        if(cacheNameForContQuery == null) {
            String cName = "cacheName-" + dateTime();

            CacheConfiguration<Object, Object> ccfg = getCacheCfg(cName);

            LOG.info(String.format("Creating cache with config %s", ccfg.toString()));

            cache = ignite().getOrCreateCache(ccfg);
        }
        else
            cache = ignite().getOrCreateCache(cacheNameForContQuery);

        for (long i = dataRangeFrom; i <= dataRangeTo; i++)
            cache.put(i, i);

        LOG.info(String.format("Finished load %d keys in cache %s", dataRangeTo - dataRangeFrom + 1, cache.getName()));

        for (int i = 0; i < registeredQryCnt; i++) {
            ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

            if (setRemoteFilterFactory)
                qry.setRemoteFilterFactory(EventFilter::new);

            qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
                @Override
                public void onUpdated(Iterable<CacheEntryEvent<? extends Object, ? extends Object>> evts) {
                    //No_op
                }
            });

            if (setLocalQuery)
                qry.setLocal(true);

            QueryCursor<Cache.Entry<Object, Object>> cursor = cache.withKeepBinary().query(qry);

            qryList.add(qry);

            cursorList.add(cursor);
        }
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        long key = getRandomKey();

        long val = key * 2;

        cache.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        for (QueryCursor<Cache.Entry<Object, Object>> cursor : cursorList) {
            if (cursor != null)
                cursor.close();
        }

//        for (QueryCursor<Cache.Entry<Object, Object>> cursor : cursorList) {
//            int cnt = 10;
//
//            while (cursor != null && cnt-- > 0)
//                sleep(1000L);
//        }

        super.tearDown();
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

    public static class EventFilter implements CacheEntryEventFilter<Object, Object> {
        @Override
        public boolean evaluate(
            CacheEntryEvent<? extends Object, ? extends Object> event) throws CacheEntryListenerException {
            return false;
        }
    }
}
