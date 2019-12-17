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

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.scenario.internal.AbstractTask;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.TaskProperties;
import org.apache.ignite.scenario.internal.exceptions.TestFailedException;
import org.apache.ignite.scenario.internal.factories.MyCacheEntryEventFilterFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Activates cluster.
 */
public class ContinuousQueryTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(ContinuousQueryTask.class.getName());

    /** */
    private IgniteCache<Integer, String> taskCache;

    private IgniteCache cache;

    private AtomicInteger cntr = new AtomicInteger();

    /** */
    private Random r = new Random();

    private Thread createDestroyThread = new Thread(new Runnable() {
        @Override public void run() {
            int i = 0;
            while (true) {
                try {
                    IgniteCache cache = ignite().createCache("test" + i);
                    cache.destroy();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                i++;
            }
        }
    });

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public ContinuousQueryTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /**
     * @throws Exception
     */
    @Override public void setUp() throws Exception {
        super.setUp();

        cache = ignite().createCache("test");

        createDestroyThread.setDaemon(true);

        createDestroyThread.start();

//        taskCache = ignite().cache("contQueryCache");

    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {

        IgniteCache<Object, Object> cache = getRandomCacheForContQuery().withKeepBinary();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Object, Object>() {
            @Override public boolean apply(Object key, Object val) {
                return true;
            }
        }));

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Object, ? extends Object>> evts) {
                for (CacheEntryEvent<? extends Object, ? extends Object> e : evts)
                    LOG.info("Updated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
            }
        });
        QueryCursor<Cache.Entry<Object, Object>> cur = cache.withKeepBinary().query(qry);
        cur.close();

        try {
            if(cache.get(cntr) != null) {
                cache.invoke(cntr.get(), new MyEntryProcessor());

                LOG.info("NOT NULL");

                cntr.getAndIncrement();
            }
        }
        catch (Exception e){
            LOG.error(String.format("Exception. cntr = %d", cntr), e);

            sleep(100);
        }

    }

    /**
     * Runs after body has finished.
     *
     * Delete lock file.
     */
    @Override public void tearDown() {
        createDestroyThread.interrupt();

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

    static class MyEntryProcessor implements EntryProcessor<Object, Object, Object> {

        static int i = 0;
        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public Object process(MutableEntry<Object, Object> entry, Object... arguments) throws EntryProcessorException {
            BinaryObjectBuilder builder;

            if (i % 2 == 0)
                builder = ignite.binary().builder("type" + i);
            else
                builder = ((BinaryObject)entry.getValue()).toBuilder();

            builder.setField("field" + i, i);
            entry.setValue(builder.build());
            i++;
            return null;
        }
    }
}
