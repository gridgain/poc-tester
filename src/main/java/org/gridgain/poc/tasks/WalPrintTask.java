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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.IgniteCallable;
import org.gridgain.poc.framework.worker.task.AbstractTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * Activates cluster.
 */
public class WalPrintTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(WalPrintTask.class.getName());

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public WalPrintTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();
    }

    /** {@inheritDoc} */
    @Override public void body0() throws TestFailedException {
        Collection<List<String>> walRecs = ignite().compute().broadcast(new WalPrinter());

        List<String> finalList = new ArrayList<>();

        for(List<String> walRec : walRecs)
            finalList.addAll(walRec);

        Collections.sort(finalList);

        for(String rec : finalList)
            LOG.info(rec);
    }

    /** {@inheritDoc} */
    public @Nullable String getTaskReport() {
        //TODO: avoid null result.
        return null;
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        super.tearDown();

        if (lockFile != null)
            new File(homeDir + SEP + lockFile).delete();
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
     *
     */
    private class WalPrinter implements IgniteCallable<List<String>> {

        @Override public List<String> call() {
            LOG.info("Apply wal");

            try {
//                return parseWalRec(0, Long.MAX_VALUE);
            }
            catch (Exception e) {
                LOG.error("WAL PRINTER", e);
            }

            return null;
        }
    }
}
