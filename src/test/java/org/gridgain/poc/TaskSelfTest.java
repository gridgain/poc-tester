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

package org.gridgain.poc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.gridgain.poc.tasks.GridTask;
import org.gridgain.poc.framework.worker.task.AbstractTask;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.framework.starter.TaskStarter;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/** */
public class TaskSelfTest {

    /**
     * @throws Exception if failed.
     */
    @Test
    @Ignore
    public void testSingleRun() throws Exception {
        DummyTask task = new DummyTask();

        List<GridTask> taskList = new ArrayList<>();

        taskList.add(task);

        TaskProperties props = new TaskProperties();

        props.setProperty(TaskProperties.TASK_NAME_PROPERTY, "Dummy-task");
        props.setProperty(TaskProperties.PARAMETER_REPORT_INTERVAL, "10");

        TaskStarter.runTask(task, props);

        assertFalse("TearDown should be called.", task.isReadyFlag.get());
        assertEquals("Task Body was not executed.", 1, task.bodyExecCnt.get());
        assertEquals(0, task.reportCnt.get());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @Ignore
    public void testTimedTask() throws Exception {
        DummyTask task = new DummyTask();

        List<GridTask> taskList = new ArrayList<>();

        taskList.add(task);

        TaskProperties props = new TaskProperties();

        props.setProperty(TaskProperties.TASK_NAME_PROPERTY, "Dummy-task");
        props.setProperty(TaskProperties.PARAMETER_REPORT_INTERVAL, "2");
        props.setProperty(TaskProperties.PARAMETER_TIME_TO_WORK, "5");

        TaskStarter.runTask(task, props);

        assertFalse("TearDown should be called.", task.isReadyFlag.get());
        assertTrue("Task Body was not executed.", task.bodyExecCnt.get() > 1);
        assertTrue("Task Body was executed too much times.", task.bodyExecCnt.get() < 12);
        assertTrue("Report was not generated.", task.reportCnt.get() > 1);
        assertTrue("Report was not generated.", task.reportCnt.get() < 3);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    @Ignore
    public void testMultithreadedTask() throws Exception {
        DummyTask task = new DummyTask();

        List<GridTask> taskList = new ArrayList<>();

        taskList.add(task);

        TaskProperties props = new TaskProperties();

        props.setProperty(TaskProperties.TASK_NAME_PROPERTY, "Dummy-task");
        props.setProperty(TaskProperties.TASK_THREADS_PROPERTY, "4");
        props.setProperty(TaskProperties.PARAMETER_REPORT_INTERVAL, "2");
        props.setProperty(TaskProperties.PARAMETER_TIME_TO_WORK, "5");

        TaskStarter.runTask(task, props);

        assertFalse("TearDown should be called.", task.isReadyFlag.get());
        assertTrue("Task Body was not executed.", task.bodyExecCnt.get() > 4);
        assertTrue("Task Body was executed too much times.", task.bodyExecCnt.get() < 4 * 12);
        assertTrue("Report was not generated.", task.reportCnt.get() > 4);
    }

    /** */
    class DummyTask extends AbstractTask implements GridTask{
        /** */
        final AtomicBoolean isReadyFlag = new AtomicBoolean();

        /** */
        final AtomicInteger bodyExecCnt = new AtomicInteger();

        /** */
        final AtomicInteger reportCnt = new AtomicInteger();

        /** */
        volatile CyclicBarrier barrier;

        @Override public void setUp() throws Exception {
            assertNotNull(barrier);

            assertTrue(isReadyFlag.compareAndSet(false, true));
        }

        @Override public void tearDown() {
            assertTrue(isReadyFlag.compareAndSet(true, false));
        }

        @Nullable @Override public String getTaskReport() {
            reportCnt.incrementAndGet();

            return "DummyReport";
        }

        /** {@inheritDoc} */
        @Override protected void body0() throws InterruptedException {
            bodyExecCnt.incrementAndGet();

            assertNotNull("Task was not initialized.", barrier);

            waitBarrier();

            assertTrue("SetUp should be called once.", isReadyFlag.get());

            Thread.sleep(500);

            waitBarrier();
        }

        /** */
        private void waitBarrier() throws InterruptedException {
            try {
                barrier.await();
            }
            catch (BrokenBarrierException e) {
                throw new AssertionError(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void init() throws IOException {
            super.init();

            assert threads > 0;
            assertNull(barrier);

            barrier = new CyclicBarrier(threads);
        }
    }
}
