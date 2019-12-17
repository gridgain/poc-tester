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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.IgniteConfiguration;

public class InvokeAllContinuousQueryTest {
    public static void main(String[] args) throws Exception {
        try {
            Ignition.start(new IgniteConfiguration().setIgniteInstanceName("server"));

            Ignition.setClientMode(true);

            Ignite ignite = Ignition.start();

            IgniteCache<MyKey, Object> cache = ignite.createCache("test-cache");

            ContinuousQuery<MyKey, Object> qry = new ContinuousQuery<>();

            final CountDownLatch[] latches = new CountDownLatch[] {
                new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1)
            };

            qry.setRemoteFilterFactory(() -> event -> true);

            qry.setLocalListener(events -> events.forEach(event -> {
                System.out.println("EVENT: " + event);

                latches[event.getKey().key()].countDown();
            }));

            cache.query(qry);

            Map<MyKey, MyEntryProcessor> map = new LinkedHashMap<>();

            map.put(new MyKey(0), new MyEntryProcessor());
            map.put(new MyKey(1), new MyEntryProcessor());
            map.put(new MyKey(2), new MyEntryProcessor());

            cache.invokeAll(map);

            for (CountDownLatch latch : latches)
                assert latch.await(2, TimeUnit.SECONDS);
        }
        finally {
            Ignition.stopAll(true);
        }
    }

    private static class MyKey {
        @AffinityKeyMapped
        private int affKey;

        private int key;

        public MyKey(int key) {
            affKey = 111;

            this.key = key;
        }

        public int key() {
            return key;
        }

        @Override
        public String toString() {
            return "MyKey [" +
                "affKey=" + affKey +
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

            return null;
        }
    }
}
