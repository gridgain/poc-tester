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

import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.binary.BinaryObject;

/**
 *
 */
public abstract class BasicTask extends AbstractTask {
    /** */
    protected IgniteAtomicSequence seq;

    /** */
    protected static final String CACHE_NAME = "MyCache";

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public BasicTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

//    BasicTask(Ignite ignite) {
//        this.ignite = ignite;
//    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        seq = ignite().atomicSequence("Counter", 0, true);
    }

    protected void sleep() {
        sleep(ThreadLocalRandom.current().nextLong(2000, 5000));
    }

    protected void sleep(long ms) {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected int nextInt() {
        return (int)seq.getAndIncrement();
    }

    public abstract String prefix();

    protected String nextString() {
        return prefix() + nextInt();
    }

    protected BinaryObject nextBinObject() {
        return ignite().binary().builder(nextString())
            .setField(nextString(), nextInt(), Integer.class)
            .build();
    }
}
