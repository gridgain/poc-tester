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
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.gridgain.poc.framework.worker.task.BasicTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * Activates cluster.
 */
public class InvokeTask extends BasicTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(InvokeTask.class.getName());

    /** */
    private static final AtomicInteger cntr = new AtomicInteger();

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public InvokeTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        try {
            int key = cntr.getAndIncrement();// nextBinObject(); DEAD LOCK HAPPENS only with INT

            getRandomCacheForContQuery().withKeepBinary().invoke(key, new MyEntryProcessor());
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override public String prefix() {
        return "invoke";
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

    static class MyEntryProcessor implements CacheEntryProcessor<Object, Object, Object> {

        static long i = 0;

        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public Object process(MutableEntry<Object, Object> entry,
            Object... arguments) throws EntryProcessorException {
            BinaryObjectBuilder builder = ignite.binary().builder("my_type");

            builder.setField("new_field" + i, i);
            entry.setValue(builder.build());
            i++;

            return null;
        }
    }
}
