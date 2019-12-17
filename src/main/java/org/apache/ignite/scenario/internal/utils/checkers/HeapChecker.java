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

package org.apache.ignite.scenario.internal.utils.checkers;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import javax.management.MBeanServer;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.utils.SSHCmdWorker;
import org.apache.ignite.scenario.internal.utils.handlers.CommandExecutionResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Checks heap size of JVM in which Ignite node runs.
 */
public class HeapChecker implements IgniteClosure<String, Long> {
    /** */
    private static final Logger LOG = LogManager.getLogger(HeapChecker.class.getName());

    /** */
    private static final String checkCls = "org.apache.ignite.scenario.internal.utils.checkers.HeapDumpChecker";

    /** */
    private PocTesterArguments args;

    /** */
    private HeapSizeCheckType checkType;

    public HeapChecker(PocTesterArguments args,
        HeapSizeCheckType checkType) {
        this.args = args;
        this.checkType = checkType;
    }

    /** {@inheritDoc} */
    @Override public Long apply(String dumpPath) {
        long startTime = System.currentTimeMillis();

        long size = 0;
        File dumpFile = new File(dumpPath);

        LOG.info("Triggering FullGC before checking heap size");

        // To make this work, do not pass "-XX:+DisableExplicitGC" JVM option
        System.gc();

        switch (checkType) {
            case JMX:
                size = getHeapUsedSizeJmx();

                break;

            case FILE_SIZE:
                LOG.info(String.format("Dump path: %s", dumpPath));

                if (dumpFile.exists())
                    dumpFile.delete();

                try {
                    dumpHeap(dumpPath, true);
                    LOG.info(String.format("Checking heap dump file size: %s", dumpPath));
                    size = dumpFile.length();
                } catch (IOException e) {
                    LOG.error(String.format("Failed to check heap dump: %s", e.getMessage()), e);
                }

                LOG.info(String.format("Count took %d millis",
                        System.currentTimeMillis() - startTime));

                break;

            case REACHABLE_INSTANCES_SIZE:
                LOG.info(String.format("Dump path: %s", dumpPath));

                if (dumpFile.exists())
                    dumpFile.delete();

                try {
                    String cp = String.format("%s/libs/*:%s/poc-tester-libs/*",
                            args.getRemoteWorkDir(), args.getRemoteWorkDir());

                    LOG.info(String.format("cp = %s", cp));

                    String javaHome = System.getProperty("java.home");

                    String cmd = String.format("%s/bin/java -cp %s %s %s", javaHome, cp, checkCls, dumpPath);

                    SSHCmdWorker worker = new SSHCmdWorker(args);

                    CommandExecutionResult res = worker.runLocCmd(cmd);

                    for (String s : res.outputList()) {
                        LOG.info(String.format("response = %s", s));

                        if (s.contains("Total_size="))
                            size = Long.valueOf(s.replace("Total_size=", ""));
                    }
                }
                catch (IOException e) {
                    LOG.error(String.format("Failed to check heap dump: %s", e.getMessage()), e);
                }

                LOG.info(String.format("Count took %d millis",
                        System.currentTimeMillis() - startTime));

                break;

            default:
                LOG.error("Unknown 'checkType' option value.");
                break;
        }

        return size;
    }


    private void dumpHeap(String filePath, boolean live) throws IOException {
        File file = new File(filePath);

        if (!file.getParentFile().exists())
            file.getParentFile().mkdirs();

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
            server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);

        mxBean.dumpHeap(filePath, live);
    }

    private long getHeapUsedSizeJmx() {
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = memBean.getHeapMemoryUsage();

        return heap.getUsed();
    }
}
