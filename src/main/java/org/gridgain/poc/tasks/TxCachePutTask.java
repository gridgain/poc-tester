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

import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.gridgain.poc.framework.worker.task.AbstractTxTask;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.worker.task.utils.TxPair;
import org.gridgain.poc.framework.model.SampleObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;

public class TxCachePutTask extends AbstractTxTask {
    private static final Logger LOG = LogManager.getLogger(TxCachePutTask.class.getName());

    public TxCachePutTask(PocTesterArguments args) {
        super(args);
    }

    public TxCachePutTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }


    @Override
    protected Callable<TxInfo> getTxBody() {
        return new TxBody(ignite());
    }

    @Override
    protected Object getLock() {
        return TxCachePutTask.class;
    }

    /**
     * Transaction body.
     */
    private class TxBody implements Callable<TxInfo> {
        @IgniteInstanceResource
        private Ignite ignite;

        TxBody(final Ignite ignite){
            this.ignite = ignite;
        }

        @Override public TxInfo call() throws Exception {
            final List<TxPair> pairList = getPairList(putGetProbArr, false);

            for (TxPair pair : pairList) {
                long key = pair.getKey0();

                SampleObject val = new SampleObject(key, fieldCnt, fieldLen);

                String cacheName0 = pair.getCacheName0();

                ignite.cache(cacheName0).put(key, val);
            }

            return new TxInfo(pairList, true, false, false);
        }
    }

}
