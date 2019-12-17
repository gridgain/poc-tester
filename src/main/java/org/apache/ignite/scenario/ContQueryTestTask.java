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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.scenario.internal.AbstractTask;
import org.apache.ignite.scenario.internal.InvokeAllContinuousQueryTest;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * Tests continuous query.
 */
public class ContQueryTestTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(ContQueryTestTask.class.getName());

    private CountDownLatch[] latches;

    private IgniteCache<MyKey, Object> cache;

    private Set<String> eventSet = new HashSet<>();



    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public ContQueryTestTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

//        cache = ignite().createCache("test-cache");
        cache = ignite().cache("cont_query_atom_part");

        ContinuousQuery<MyKey, Object> qry = new ContinuousQuery<>();

        qry.setRemoteFilterFactory(() -> event -> true);

        qry.setLocalListener(events -> events.forEach(event -> {
            LOG.info("EVENT: " + event);

            eventSet.add(event.toString().substring(0, 76));

            latches[event.getKey().key()].countDown();
        }));

        cache.query(qry);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        latches = new CountDownLatch[] {
            new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1)
        };

        Map<MyKey, MyEntryProcessor> map = new LinkedHashMap<>();

        map.put(new MyKey(0), new MyEntryProcessor());
        map.put(new MyKey(1), new MyEntryProcessor());
        map.put(new MyKey(2), new MyEntryProcessor());

        cache.invokeAll(map);

        for (CountDownLatch latch : latches) {
            LOG.info(String.format("Latch = %s", latch));

            try {
                if(latch.await(2, TimeUnit.SECONDS))
                    LOG.warn("Timeout is exceeded");
            }
            catch (InterruptedException e){
                LOG.error(e.getMessage());
            }
        }

    }

    /** {@inheritDoc} */
    @Nullable public String getTaskReport() {
        //TODO: avoid null result.
        return null;
    }

    @Override public void tearDown() {
        super.tearDown();

        LOG.info(String.format("SetSize = %d", eventSet.size()));

        for(String str : eventSet)
            LOG.info(String.format("Set content = %s", str));

        if(eventSet.contains("CacheContinuousQueryEvent [evtType=UPDATED, key=MyKey [affKey=affKey, key=1]") ||
            eventSet.contains("CacheContinuousQueryEvent [evtType=CREATED, key=MyKey [affKey=affKey, key=1]"))
            LOG.info("Continuous query has worked ok;");
        else
            LOG.info("Continuous query has failed;");

    }

    /** */
    @Override protected void addPropsToMap(TaskProperties props){
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>) propMap.get("headersMap");

        hdrMap.put("unit", "boolean");
        hdrMap.put("data", "status");

        propMap.put("reportDir", "reports");
    }

    private static class MyKey {
//        @AffinityKeyMapped
//        private int affKey;

        private int key;

        public MyKey(int key) {
//            affKey = 111;

            this.key = key;
        }

        public int key() {
            return key;
        }

        @Override
        public String toString() {
            return "MyKey [" +
                "affKey=" + "affKey" +
                ", key=" + key +
                ']';
        }
    }

    private static class MyValue {
        private int value;

        public MyValue(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "MyValue [" +
                "value=" + value +
                ']';
        }
    }

    private static class MyEntryProcessor implements EntryProcessor<MyKey, Object, Object> {
        @Override public Object process(MutableEntry<MyKey, Object> entry, Object... args) {
            int key = entry.getKey().key;

            entry.setValue(key == 1 ? new MyValue(111) : true);
//            entry.setValue(key == 1 ? "set" : true);

            return null;
        }
    }
}
