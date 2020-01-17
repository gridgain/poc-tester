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

import java.util.concurrent.TimeUnit;

import org.apache.ignite.IgniteClientDisconnectedException;
import org.gridgain.poc.tasks.GridTask;
import org.gridgain.poc.framework.exceptions.TestFailedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import javax.cache.CacheException;

import static org.gridgain.poc.framework.worker.task.TaskLooper.State.INIT;
import static org.gridgain.poc.framework.worker.task.TaskLooper.State.RUNNING;
import static org.gridgain.poc.framework.worker.task.TaskLooper.State.STOPPED;

/**
 *
 */
public class TaskLooper implements Runnable {
    /** */
    private static final Logger LOG = LogManager.getLogger(TaskLooper.class.getName());

    /** */
    private final GridTask task;

    /** */
    private String taskName;

    /** (in secs) */
    private long timeToWork;

    /** */
    private volatile Thread taskThread;

    /** */
    private volatile int iterCnt;

    /** */
    private volatile State state = INIT;

    /** */
    public TaskLooper(GridTask task, String taskName, long timeToWork) {
        this.task = task;
        this.taskName = taskName;
        this.timeToWork = timeToWork;
    }

    /** Get task state for long running tasks. */
    public @NotNull String getTaskReport() throws Exception {
        if (state == INIT)
            return "Task thread is not started yet:" + taskName;
        else if (state == STOPPED)
            return "Task thread finished:" + taskName;

        String report = task.getTaskReport();

        return "Task thread " + String.valueOf(taskThread.getName()) + " completed " +
            iterCnt + " operations" +
            ((report == null) ? '.' : ": " + report);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        taskThread = Thread.currentThread();

        final String threadName = taskThread.getName();

        LOG.info("Task thread started: name=" + threadName);

        try {
            state = RUNNING;

            final long taskEndTime = timeToWork < 0 ?
                    Long.MIN_VALUE :
                    System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeToWork);

            while (!Thread.interrupted() && state == RUNNING) {
                try {
                    task.body();
                }
                catch (IgniteClientDisconnectedException e) {
                    LOG.warn("Client disconnected. Waiting for reconnection", e);
                    e.reconnectFuture().get();
                    break;
                }
                catch (CacheException e) {
                    LOG.warn(e);
                    if (e.getCause() instanceof IgniteClientDisconnectedException) {
                        IgniteClientDisconnectedException cause = (IgniteClientDisconnectedException)e.getCause();
                        LOG.warn("Client disconnected. Waiting for reconnection", cause);
                        cause.reconnectFuture().get();
                        break;
                    }
                }
                catch (InterruptedException ignore) {
                    break;
                }
                catch (TestFailedException e) {
                    LOG.error("Test failed", e);
                    state = STOPPED;
                    task.tearDown();
                }

                iterCnt++;

                if (timeToWork < 0 || System.currentTimeMillis() > taskEndTime)
                    break;
            }
        }
        catch (Throwable t) {
            LOG.error("Unexpected error occurs: threadName=" + threadName, t);
        }
        finally {
            state = STOPPED;
        }

        LOG.info(String.format("Task thread %s has finished %d operations completed.", threadName, iterCnt));
    }

    /** */
    public boolean isDone() {
        return state == STOPPED;
    }

    /** */
    protected enum State {
        /** */
        INIT,

        /** */
        RUNNING,

        /** */
        STOPPED
    }
}
