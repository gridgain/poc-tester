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

package org.gridgain.poc.framework.worker.task;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Fill cache with generated data: - key is random UUID. - value is generated BinaryObject with random String fields.
 * Parameters: size fields fieldSize
 */
public abstract class AbstractLoadTask extends AbstractReportingTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(AbstractLoadTask.class.getName());

    /** */
    private static final int DFLT_LOAD_THREADS = 1;

    /** Field content size. */
    protected int loadThreads = DFLT_LOAD_THREADS;

    /** Keep lock flag */
    protected AtomicBoolean keepLock = new AtomicBoolean();

    /** */
    private long lastCachesSize;

    /** */
    protected boolean cpOnFailedLoad;

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public AbstractLoadTask(PocTesterArguments args) {
        super(args);
    }

    /**
     * Constructor.
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public AbstractLoadTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        loadThreads = props.getInteger("loadThreads", DFLT_LOAD_THREADS);

        cpOnFailedLoad = props.getBoolean("cpOnFailedLoad", Boolean.FALSE);

    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        if (lockFile != null)
            createLockFile();

        setTaskLoad();

        String startRep = String.format("%s %d of %d is starting load. Will load keys from %d to %d",
            getClass().getSimpleName(), nodeCntr + 1, totalNodesNum, taskLoadFrom, taskLoadTo);

        LOG.info(startRep);
        System.out.println(startRep);

        super.setUp();

        lastCachesSize = 0L;
    }

    /** {@inheritDoc} */
    @Override public String reportString() {
        long sum = getTotalCachesSize();

        long delta = sum - lastCachesSize;

        lastCachesSize = sum;

        return String.valueOf(delta);
    }

    /**
     * Runs after body has finished.
     *
     * Delete lock file.
     */
    @Override public void tearDown() {
        if (lockFile != null && !keepLock.get())
            new File(homeDir + SEP + lockFile).delete();
        else
            LOG.error("Load is not completed. Will not delete lock file.");


        LOG.info(String.format("%s has finished load. Total number of entries in %d caches is %d",
            this.getClass().getSimpleName(), getCacheNameList().size(), getTotalCachesSize()));

        super.tearDown();
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        Map<String, String> hdrMap = (Map<String, String>) propMap.get("headersMap");

        hdrMap.put("unit", "entry");
        hdrMap.put("data", "delta");
    }

    /**
     * Set task load range for each client.
     */
    private void setTaskLoad(){
        if(totalNodesNum == 0){
            LOG.error("Total nodes number can't be 0!");
            tearDown();
        }

        if (args.getTotalNodesNum() == 1) {
            taskLoadFrom = dataRangeFrom;
            taskLoadTo = dataRangeTo;
        }
        else {
            long totalLoad = dataRangeTo - dataRangeFrom;

            long batch = totalLoad / totalNodesNum;

            long shift = batch * nodeCntr;

            taskLoadFrom = nodeCntr == 0 ? dataRangeFrom : dataRangeFrom + shift + 1;

            taskLoadTo = nodeCntr == totalNodesNum - 1 ? dataRangeTo : dataRangeFrom + shift + batch;
        }
    }
}
