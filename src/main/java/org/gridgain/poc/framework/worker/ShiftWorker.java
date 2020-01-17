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

package org.gridgain.poc.framework.worker;

import java.io.File;
import java.util.List;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.println;

/**
 * Class for collecting data from remote hosts.
 */
public class ShiftWorker extends AbstractStartWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(ShiftWorker.class.getName());

    /** */
    private File logDir;

    /** */
    public ShiftWorker() {
        super();

        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    /** */
    public static void main(String[] args) {
        ShiftWorker worker = new ShiftWorker();

        worker.work(args);
    }

    /**
     * Print help.
     */
    protected void printHelp() {
        System.out.println(" Copied 'work' directories on server hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" shift.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -wd   || --remoteWorkDir   <path to upload zip file and run poc-tester in>");
        System.out.println(" -shft || --shiftNum        <number for shifted work directory>");
        System.out.println(" -back || --shiftBack       <flag for shifting work directory back>");
        System.out.println();
    }

    /** {@inheritDoc} */
    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getHostList(args.getServerHosts(), uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {

    }

    /** */
    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total, String consID) {
        final String homeParent = new File(rmtHome).getParent();

        final String storDir = String.format("%s/shifted-dirs", homeParent);

        String destName = null;

        String srcName = null;

        if(args.isShiftBack()) {
            destName = rmtHome;

            srcName = String.format("%s/work-%d/work", storDir, args.getShiftNum());
        }
        else {
            destName = String.format("%s/work-%d", storDir, args.getShiftNum());

            srcName = String.format("%s/work", rmtHome);
        }

        println(String.format("Started to copy dir %s to dir %s on the host %s", srcName, destName, host));


        final RemoteSshExecutor worker = new RemoteSshExecutor(args);

        final String delCmd = String.format("rm -rf %s/work", destName);

        try {
            worker.runCmd(host, delCmd);

            final String mkdirCmd = String.format("mkdir -p %s", destName);

            worker.runCmd(host, mkdirCmd);

            final String cpCmd = String.format("cp -r %s %s", srcName, destName);

            worker.runCmd(host, cpCmd);

            println(String.format("Done for the host %s", host));
        }
        catch (Exception e){
            LOG.error(String.format("Failed to copy directories on the host %s", host), e);

        }
    }
}
