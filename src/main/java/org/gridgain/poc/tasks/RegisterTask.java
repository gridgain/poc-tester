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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.gridgain.poc.framework.worker.task.AbstractTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.gridgain.poc.framework.utils.PocTesterUtils.sleep;

/**
 * Activates cluster.
 */
public class RegisterTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(RegisterTask.class.getName());

    private Random rand;

    /** */
    private static final String SEQ_NUM_FLD = "f0";

    /** */
    private static final int UPDATES_COUNT = 50;

    /** */
    private static final String BINARY_TYPE_NAME = "TestBinaryType";

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public RegisterTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /**
     *
     * @throws Exception
     */
    @Override public void setUp() throws Exception {
        super.setUp();

        rand = new Random();

    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        boolean done = false;

        int cnt = 0;

        while (!done && cnt++ < 300) {
            try {
//                sleep(10L);
                ignite().compute().callAsync(new BinaryObjectAdder((IgniteEx)ignite(), 5000));

               done = true;
            }
            catch (Exception e){
                LOG.error(String.format("Failed to send compute job. Will try again. Cnt = %d", cnt), e);

                sleep(100L);
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable public String getTaskReport() {
        //TODO: avoid null result.
        return null;
    }

    /** */
    @Override protected void addPropsToMap(TaskProperties props){
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>) propMap.get("headersMap");

        hdrMap.put("unit", "boolean");
        hdrMap.put("data", "status");

        propMap.put("reportDir", "reports");
    }

    /**
     * Instruction for node to perform <b>add new binary object</b> action on cache in <b>keepBinary</b> mode.
     *
     * Instruction includes id the object should be added under, new field to add to binary schema
     * and {@link FieldType type} of the field.
     */
    private static final class BinaryUpdateDescription {
        /** */
        private int itemId;

        /** */
        private String fieldName;

        /** */
        private FieldType fieldType;

        /**
         * @param itemId Item id.
         * @param fieldName Field name.
         * @param fieldType Field type.
         */
        private BinaryUpdateDescription(int itemId, String fieldName, FieldType fieldType) {
            this.itemId = itemId;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }
    }

    /**
     *
     */
    private enum FieldType {
        /** */
        NUMBER,

        /** */
        STRING,

        /** */
        ARRAY,

        /** */
        OBJECT
    }

    /**
     * Generates random number to use when creating binary object with field of numeric {@link FieldType type}.
     */
    private static int getNumberFieldVal() {
        return ThreadLocalRandom.current().nextInt(100);
    }

    /**
     * Generates random string to use when creating binary object with field of string {@link FieldType type}.
     */
    private static String getStringFieldVal() {
        return "str" + (100 + ThreadLocalRandom.current().nextInt(9));
    }

    /**
     * Generates random array to use when creating binary object with field of array {@link FieldType type}.
     */
    private static byte[] getArrayFieldVal() {
        byte[] res = new byte[3];
        ThreadLocalRandom.current().nextBytes(res);
        return res;
    }

    /**
     * @param builder Builder.
//     * @param desc Descriptor with parameters of BinaryObject to build.
     * @return BinaryObject built by provided description
     */
    private static BinaryObject newBinaryObject(BinaryObjectBuilder builder) {
        builder.setField(SEQ_NUM_FLD, 1);

//        switch (desc.fieldType) {
//            case NUMBER:
//                builder.setField(desc.fieldName, getNumberFieldVal());
//                break;
//            case STRING:
//                builder.setField(desc.fieldName, getStringFieldVal());
//                break;
//            case ARRAY:
//                builder.setField(desc.fieldName, getArrayFieldVal());
//                break;
//            case OBJECT:
//                builder.setField(desc.fieldName, new Object());
//        }

        return builder.build();
    }

//    /**
//     * @param builder Builder.
//     * @param desc Descriptor with parameters of BinaryObject to build.
//     * @return BinaryObject built by provided description
//     */
//    private static BinaryObject newBinaryObject(BinaryObjectBuilder builder, BinaryUpdateDescription desc) {
//        builder.setField(SEQ_NUM_FLD, desc.itemId + 1);
//
//        switch (desc.fieldType) {
//            case NUMBER:
//                builder.setField(desc.fieldName, getNumberFieldVal());
//                break;
//            case STRING:
//                builder.setField(desc.fieldName, getStringFieldVal());
//                break;
//            case ARRAY:
//                builder.setField(desc.fieldName, getArrayFieldVal());
//                break;
//            case OBJECT:
//                builder.setField(desc.fieldName, new Object());
//        }
//
//        return builder.build();
//    }

    /**
     * Compute job executed on each node in cluster which constantly adds new entries to ignite cache
     * according to {@link BinaryUpdateDescription descriptions} it reads from shared queue.
     */
    private final class BinaryObjectAdder implements IgniteCallable<Object> {
        /** */
        private final IgniteEx ignite;

        /** */
        private final long timeout;

        /** */
        private Random r = new Random();

        /**
         * @param ignite Ignite.
         * @param timeout Timeout.
         */
        BinaryObjectAdder(IgniteEx ignite, long timeout) {
            this.ignite = ignite;
            this.timeout = timeout;
        }


        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
//            START_LATCH.await();

            boolean done = false;

            int cnt = 0;

            IgniteCache<Object, Object> cache;

            while(!done && cnt++ < 300) {
                try {
                    cache = getRandomCacheForContQuery().withKeepBinary();

//                    BinaryUpdateDescription[] updatesArr = new BinaryUpdateDescription[50];
//
//                    for (int i = 0; i < 50; i++) {
//                        FieldType fType = null;
//
//                        int f = r.nextInt(4);
//
//                        switch (f) {
//                            case 0:
//                                fType = FieldType.NUMBER;
//                                break;
//                            case 1:
//                                fType = FieldType.STRING;
//                                break;
//                            case 2:
//                                fType = FieldType.ARRAY;
//                                break;
//                            case 3:
//                                fType = FieldType.OBJECT;
//                        }
//
//                        updatesArr[i] = new BinaryUpdateDescription(i, "f" + (f + 1), fType);
//                    }

                    int i = 0;

                    while (i<10) {
//                        BinaryUpdateDescription desc = updatesArr[r.nextInt(50)];

//                        if (desc == null)
//                            break;

                        BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME + i + r.nextInt());

                        i++;

//                        int key = desc.itemId;

                        BinaryObject bo = newBinaryObject(builder);
//                        BinaryObject newVal = bo.toBuilder().setField("new_field", "my_string", String.class).build();

                        if (i % 1 == 0) {
                            cache.put(i, bo);
//                            cache.put(key, newVal);
//                        } else {
//                            cache.invoke(key, new CacheEntryProcessor<Object, Object, Object>() {
//                                @Override
//                                public Object process(MutableEntry<Object, Object> entry,
//                                    Object... objects) throws EntryProcessorException {
//
//                                    entry.setValue(objects[0]);
//
//                                    return null;
//                                }
//                            }, bo);
//                            cache.invoke(key, new CacheEntryProcessor<Object, Object, Object>() {
//                                @Override
//                                public Object process(MutableEntry<Object, Object> entry,
//                                    Object... objects) throws EntryProcessorException {
//
//                                    entry.setValue(objects[0]);
//
//                                    return null;
//                                }
//                            }, newVal);
                        }

//                LOG.info(String.format("Cache size = %d", cache.size()));
//                LOG.info(String.format("Queue size = %d", updatesQueue.size()));
                    }

//                    LOG.info(String.format("Cache size = %d", cache.size()));
                }
                catch (Exception e){
                    LOG.error(String.format("Failed to get cache instance. Will try again. Cnt = %d", cnt), e);

                    sleep(1000L);
                }
            }

//            if (updatesQueue.isEmpty())
//                FINISH_LATCH_NO_CLIENTS.countDown();


            return null;
        }
    }
}
