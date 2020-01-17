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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.gridgain.poc.framework.worker.task.AbstractLoadTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.gridgain.poc.framework.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Fill cache with generated data.
 */
public class PutAllLoadTask extends AbstractLoadTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(PutAllLoadTask.class.getName());

    /** */
    private AtomicInteger cacheCnt = new AtomicInteger();

    /** */
    public PutAllLoadTask(PocTesterArguments args,
        TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        assert fieldCnt > 0;

        Ignite ignite = ignite();

        ExecutorService execServ = Executors.newFixedThreadPool(loadThreads);

        List<Future<?>> futList = new ArrayList<>();

        for (String cacheName : ignite.cacheNames()) {
            if (!checkNameRange(cacheName))
                continue;

            futList.add(execServ.submit(new Loader(cacheName)));
        }

        for (Future f : futList) {
            try {
                f.get();
            }
            catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        execServ.shutdown();
    }

    /**
     * Loader implementation.
     */
    private class Loader implements Runnable {
        /** */
        private final String cacheName0;

        /** */
        Loader(String cacheName0) {
            this.cacheName0 = cacheName0;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            long currKey = taskLoadFrom;

            while (currKey <= taskLoadTo) {
                int batchSize0 = 0;

                Map<Long, SampleObject> toPut = new HashMap<>();

                while (batchSize0 < batchSize && currKey <= taskLoadTo) {
                    toPut.put(currKey, new SampleObject(currKey, fieldCnt, fieldLen));

                    batchSize0++;

                    currKey++;

                    if (currKey % 10_000 == 0)
                        LOG.info(String.format("%s key has been loaded into cache %s.",
                            currKey, cacheName0));
                }

                ignite().cache(cacheName0).putAll(toPut);
            }

            LOG.info(String.format("Task has loaded data in %d caches.", cacheCnt.getAndIncrement()));
        }
    }
}
