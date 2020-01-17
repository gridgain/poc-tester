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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.gridgain.poc.framework.worker.task.AbstractTxTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.worker.task.utils.TxPair;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.gridgain.poc.framework.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runs put and get
 */
public class PutGetCacheTask extends AbstractTxTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(PutGetCacheTask.class.getName());

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public PutGetCacheTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        if (!waitIfStopFlag("pauseTX"))
            return;

        txLenCalcMode = TxLenCalcMode.PREDEFINED;

        txLenNum = 1;

        List<TxPair> pairList = getPairList(putGetProbArr, false);

        TxPair pair = pairList.get(0);

        long key0 = pair.getKey0();
        long key1 = pair.getKey1();

        String cacheName0 = pair.getCacheName0();
        String cacheName1 = pair.getCacheName1();

        SampleObject val = (SampleObject)ignite().cache(cacheName0).get(key0);

        if(val == null)
            LOG.error(String.format("Failed to find value for key %d in cache %s. Make sure all data was loaded properly.",
                    key0, cacheName0));

        try {
            ignite().cache(cacheName1).put(key1, val);
        }
        catch (Exception e) {
            LOG.error("Unexpected error occurs.", e);

            try {
                Thread.sleep(1000L);
            }
            catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>) propMap.get("headersMap");

        hdrMap.put("unit", "entry");
        hdrMap.put("data", "latency");
        hdrMap.put("ops", "operations");

        propMap.put("reportDir", "eventstore");
    }

    /** {@inheritDoc} */
    @Override protected Object getLock() {
        return PutGetCacheTask.class;
    }

    /** {@inheritDoc} */
    @Override protected Callable<TxInfo> getTxBody() {
        return null;
    }
}
