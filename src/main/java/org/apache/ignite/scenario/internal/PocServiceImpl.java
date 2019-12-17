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

package org.apache.ignite.scenario.internal;

import java.util.Random;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.scenario.RegisterTask;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

public class PocServiceImpl implements Service, PocService {
    /** Auto-injected instance of Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Distributed cache used to store counters. */
    private IgniteCache<String, Integer> cache;

    /** Service name. */
    private String svcName;
    private Random r = new Random();

    /**
     * Service initialization.
     */
    @Override public void init(ServiceContext ctx) {
        // Pre-configured cache to store counters.
        cache = ignite.cache("myCounterCache");

        svcName = ctx.name();

        System.out.println("Service was initialized: " + svcName);
    }

    /**
     * Cancel this service.
     */
    @Override public void cancel(ServiceContext ctx) {
        // Remove counter from cache.
        cache.remove(svcName);

        System.out.println("Service was cancelled: " + svcName);
    }

    /**
     * Start service execution.
     */
    @Override public void execute(ServiceContext ctx) {
       cache.withKeepBinary().get(newBinaryObject());
    }

    /**
     * @return BinaryObject built by provided description
     */
    private BinaryObject newBinaryObject() {

        int idx = r.nextInt();

        BinaryObjectBuilder builder = ignite.binary().builder("builder" + idx);

        builder.setField("field" + idx, "value" + idx);

        return builder.build();
    }

    @Override public int get() throws CacheException {
        Integer i = cache.get(svcName);

        return i == null ? 0 : i;
    }

    @Override public int increment() throws CacheException {
        return cache.invoke(svcName, new CounterEntryProcessor());
    }

    /**
     * Entry processor which atomically increments value currently stored in cache.
     */
    private static class CounterEntryProcessor implements EntryProcessor<String, Integer, Integer> {
        @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {
            int newVal = e.exists() ? e.getValue() + 1 : 1;

            // Update cache.
            e.setValue(newVal);

            return newVal;
        }
    }
}
