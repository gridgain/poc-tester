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

package org.gridgain.poc.framework.starter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.worker.task.TaskLooper;
import org.gridgain.poc.framework.worker.task.TaskProperties;
import org.gridgain.poc.tasks.GridTask;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task runner class.
 */
public final class TaskStarter implements Runnable {
    /** */
    private static final Logger LOG = LogManager.getLogger(TaskStarter.class.getName());

    /** */
    private static final String DEFAULT_PACKAGE = "org.gridgain.poc.tasks";

    /** */
    private static final String DEFAULT_CLS_NAME = "ActivateTask";

    private static final long CLIENT_RECONNECT_TIMEOUT = 10L * 60 * 1000;

    /** (in secs) */
    private long timeToWork;

    /** (in millis) */
    private long reportInterval;

    /** */
    private final GridTask task;

    /** */
    private PocTesterArguments args;

    /** */
    private int threadNum;

    /** */
    private List<TaskLooper> taskThreads;

    /** */
    private String taskName;

    /** */
    private List<GridTask> taskList;

    /** */
    private Ignite ignite;

    /** */
    private String lastReport;

    /** */
    private TaskStarter(GridTask task) {
//        timeToWork = task.getTimeToWork();
//        reportInterval = task.getReportInterval();
//        taskName = task.getTaskName();
        this.task = task;

//        System.out.println("DEBUG_MSG threads = " + task.getThreads());


    }

    /**
     * Main.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) throws Exception {
        try {
            runTask(args);
        }
        catch (Exception e) {
            LOG.error("Exception in task.", e);

            e.printStackTrace();
        }
    }

    /**
     * Run task.
     *
     * @param args Command line arguments.
     */
    private static void runTask(String[] args) throws Exception {
        if (args.length == 0) {
            LOG.error("Task cannot start without arguments.");

            return;
        }

        PocTesterArguments args0 = new PocTesterArguments();
        args0.setArgsFromCmdLine(args, "TaskStarter");
        args0.setArgsFromStartProps();

        TaskProperties props;

        if (args0.getTaskProperties() != null)
            props = parseProperties(args0.getTaskProperties());
        else {
            LOG.error("You cannot start task without defining property file.");

            return;
        }

        String taskPcg = props.getProperty("taskPackage", DEFAULT_PACKAGE);

        String clsName = props.getProperty("MAIN_CLASS", DEFAULT_CLS_NAME);

        if (args0.getIdeaDebugPath() != null)
            setDebug(clsName, args0);

        String clsNameFull = String.format("%s.%s", taskPcg, clsName);

        Class<?> cls = Class.forName(clsNameFull);

        if (!GridTask.class.isAssignableFrom(cls))
            throw new IllegalArgumentException();

        GridTask task = (GridTask)cls.getDeclaredConstructor(PocTesterArguments.class, TaskProperties.class)
            .newInstance(args0, props);

//        task.setArgs(args0);

        runTask(task, props);
    }

    /**
     *
     * @param props Task properties.
     */
    private void setFields(TaskProperties props){
        timeToWork = props.getLong("timeToWork", -1L);

        reportInterval = props.getLong("reportInterval", 1L) * 1000L;

        taskName = props.getString("MAIN_CLASS", "unknown");

        threadNum = props.getInteger("threads", 1);

        taskThreads = new ArrayList<>(threadNum);

        for (int i = 0; i < threadNum; i++) {
            TaskLooper looper = new TaskLooper(task, taskName, timeToWork);

            taskThreads.add(looper);
        }
    }

    /**
     * Helper method.
     *
     * @param args args.
     */
    private static void setDebug(String name, PocTesterArguments args) {
        if (System.getProperty("pocTesterHome") == null)
            System.setProperty("pocTesterHome", args.getIdeaDebugPath());

        System.setProperty("clientDirName", String.format("task-%s-%s", PocTesterUtils.dateTime(),
            name));

        try {
            new File(String.format("%s/runner.log", args.getIdeaDebugPath())).createNewFile();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Run task.
     *
     * @param task task to run.
     * @param props task properties.
     */
    public static void runTask(GridTask task, TaskProperties props) throws Exception {
//        LOG.debug("Init task: " + task.getTaskName()); //TODO: make debug.

        task.init();

//            System.out.println("DEBUG_MSG before setup");

        task.setUp();

//            System.out.println("DEBUG_MSG after setup");

        TaskStarter runner = new TaskStarter(task);

        runner.setFields(props);

        runner.run();

    }

    /**
     * Parse task parameters into properties.
     *
     * @param propPath Path to property file.
     */
    private static TaskProperties parseProperties(String propPath) throws IOException {
        TaskProperties props = new TaskProperties();

        LOG.info("Property file path: " + propPath);

        props.load(new FileInputStream(propPath));

//        if(props.getProperty("timeToWork")!=null)
//            LOG.info("timeToWork: " + props.getProperty("timeToWork"));

//        props.setProperty(TASK_NAME_PROPERTY, getTaskName(propPath));

        return props;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        assert timeToWork != 0;

        LOG.info("Run task: [name=" + taskName + ", threads=" + threadNum + ", timeToWork=" + timeToWork
            + ", reportInterval=" + reportInterval + ']');

        ThreadFactory threadFactory = new IgniteThreadFactory("TaskPool", taskName);

        IgniteThread reporter = new IgniteThread("", "Reporter-thread", new Reporter());

        reporter.start();

        try {
            LOG.debug("Setup task: " + taskName);

            List<Thread> threads = new ArrayList<>(threadNum);

            int cnt = 0;

            for (TaskLooper looper : taskThreads) {
                Thread t = threadFactory.newThread(looper);

                t.setName(taskName + "-threadNum-" + cnt++);

                threads.add(t);

                t.start();
            }

            join(threads.toArray(new Thread[threads.size()]));

            join(reporter);
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to run task. Reason: %s", e.getMessage()), e);

            for (StackTraceElement s : e.getStackTrace())
                LOG.error(s.toString());
        }
        finally {
            task.tearDown();
        }
    }

    /**
     * @param threads Threads to shutdown.
     */
    private void join(Thread... threads) {
        for (Thread t : threads) {
            try {
                t.join();
            }
            catch (InterruptedException e) {
                LOG.log(Level.WARN, e.getMessage(), e);
            }
        }
    }

    /**
     * @return {@code True} if there is at least one non-finished task.
     */
    private boolean hasActiveTask() {
        for (TaskLooper task : taskThreads) {
            if (!task.isDone())
                return true;
        }

        return false;
    }

    /** */
    private synchronized void logTaskReport() throws Exception {
        StringBuilder sb = new StringBuilder("Task report " + taskName + ":\n");

        try {
            for (TaskLooper task : taskThreads)
                sb.append('\n').append(task.getTaskReport()).append('\n');
        }
        catch (Exception e) {
            LOG.error("Failed to get task report.", e);
            throw e;
        }

        String report = sb.toString();

        if (!report.equals(lastReport))
            LOG.info(report);

        lastReport = report;
    }

    /** */
    private class Reporter implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            long currTime = System.currentTimeMillis();
            long lastReportTime = currTime;

            while (!Thread.interrupted() && hasActiveTask()) {
                try {
                    if ((currTime = System.currentTimeMillis()) >= lastReportTime + reportInterval) {
                        logTaskReport();

                        lastReportTime = currTime;
                    }
                }
                catch (IgniteClientDisconnectedException e) {
                    LOG.warn("Client disconnected from cluster. Will wait for reconnect", e);
                    e.reconnectFuture().get(CLIENT_RECONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
                    break;
                }
                catch (Exception e) {
                    LOG.error(e);
                    break;
                }

                try {
                    Thread.sleep(reportInterval); //TODO: rework to futures.
                }
                catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
