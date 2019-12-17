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

package org.apache.ignite.scenario.internal.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.ignite.scenario.internal.PocTesterArguments;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.checkList;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.hms;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;

/**
 * Created by oostanin on 12.01.18.
 */
public abstract class AbstractStartWorker extends AbstractWorker {
    /** */
    protected boolean uniqHosts;

    /** */
    protected int startThreads;

    /** */
    private static final long DFLT_TIMEOUT = 600_000;

    /**
     * Set start threads.
     *
     * @param startThreads number of start threads.
     */
    public void setStartThreads(int startThreads) {
        this.startThreads = startThreads;
    }

    /**
     * Work method.
     *
     * @param argsArr Array of arguments from command line.
     */
    protected void work(String[] argsArr) {
        handleArgs(argsArr);

        beforeWork(args);

        workOnHosts(args);
    }

    /**
     * Argument handler.
     *
     * @param argsArr Array of arguments from command line.
     */
    void handleArgs(String[] argsArr){
        args = new PocTesterArguments();

        // Set PocTesterArgument object.
        args.setArgsFromCmdLine(argsArr);
        args.setArgsFromStartProps();

        // If number of start threads is defined in command line arguments set number of threads otherwise number of
        // threads will be computed for worker by its own implementation.
        if (args.getStartThreads() != null)
            setStartThreads(args.getStartThreads());

        if (args.getRemoteWorkDir() != null)
            rmtHome = args.getRemoteWorkDir();
        else {
            String msg = hms() + "Remote work directory is not defined. Use -h to read help";

            System.out.println(msg);

            LOG.error(msg);

            System.exit(1);
        }

        if (args.getGrepLines() != null)
            addGrepLines(args.getGrepLines());
    }

    /**
     * Creates list of all server and  client host addresses.
     *
     * @param args Arguments.
     * @return List of all server and  client host addresses.
     */
    protected abstract List<String> getHostList(PocTesterArguments args);

    /**
     * Method to execute before starting actual work method.
     *
     * @param args Arguments.
     */
    protected abstract void beforeWork(PocTesterArguments args);

    /**
     * Executes start method defined in worker class asynchronously.
     *
     * @param args Arguments.
     */
    void workOnHosts(final PocTesterArguments args) {
        final String dateTime = PocTesterUtils.dateTime();

        final List<String> hostList = getHostList(args);

        for(String host : new HashSet<>(hostList))
            semMap.put(host, new Semaphore(1));

        if (!checkList(hostList))
            throw new RuntimeException("Specified list is incorrect:" + hostList);

        ExecutorService execServ = Executors.newFixedThreadPool(startThreads);

        Collection<Future<?>> futList = new ArrayList<>();

        for (int cntr = 0; cntr < hostList.size(); cntr++) {
            if (!checkHost(hostList.get(cntr))) {
                printer(String.format("Host ip '%s' does not match ip address pattern.", hostList.get(cntr)));

                continue;
            }

            final int cntrF = cntr;

            futList.add(execServ.submit(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    String host = hostList.get(cntrF);

                    Thread.currentThread().setName(String.format("Starter-%s", host));

                    Semaphore sem = semMap.get(host);

                    sem.acquire();

                    start(args, host, dateTime, cntrF, hostList.size(), null);

                    sem.release();

                    return null;
                }
            }));
        }

        for (Future f : futList) {
            try {
                f.get(DFLT_TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException e) {
                LOG.error("Abstract start worker: timeout is exceeded.", e);
            }
            catch (Exception e) {
                LOG.error("Failed to start worker.", e);
            }
        }

        execServ.shutdown();
    }

    /**
     * Start method should be defined in each worker class.
     *
     * @param args Arguments.
     * @param host Host IP address.
     * @param dateTime Date+time string.
     * @param cntr Counter.
     * @param total Total number of worker processes starting.
     * @param consID Consistent ID.
     * @throws Exception if failed.
     */
    public abstract void start(PocTesterArguments args, String host, String dateTime, int cntr, int total,
        String consID) throws Exception;

    /**
     * Add grep lines to search in log files.
     *
     * @param lines String lines.
     */
    private void addGrepLines(String lines) {
        String[] lineArr = lines.split(",");

        linesToLook.addAll(Arrays.asList(lineArr));
    }

    /**
     * Check if host address is a valid IP address.
     *
     * @param host {@code String} host address.
     * @return {@code true} if host address is a valid IP address or {@code false} if not.
     */
    private boolean checkHost(String host) {
        Pattern ptrn = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

        return "localhost".equals(host) || ptrn.matcher(host).matches();
    }

    /**
     * Check if defined config file is actually exists.
     *
     * @param args Arguments.
     * @param path Path to config file.
     */
    void checkConfig(PocTesterArguments args, String path) {
        rmtHome = args.getRemoteWorkDir();

        String cfg = path.startsWith(rmtHome) ? path :
            String.format("%s/%s", rmtHome, path);

        String locCfgPath = cfg.replace(rmtHome, locHome);

        File toCheck = new File(locCfgPath);

        if (!toCheck.exists()) {
            printer(String.format("Configuration or property file %s is not found. Cannot start node.",
                toCheck.getAbsolutePath()));

            System.exit(1);
        }
    }
}
