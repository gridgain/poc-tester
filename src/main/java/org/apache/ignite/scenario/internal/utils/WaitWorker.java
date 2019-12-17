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

import java.util.List;
import org.apache.ignite.scenario.internal.PocTesterArguments;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Created by oostanin on 09.12.17.
 */
public class WaitWorker extends AbstractStartWorker {

    public WaitWorker() {
        super();

        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    public static void main(String[] args) {
        new WaitWorker().work(args);
    }

    /**
     * Print help.
     */
    protected void printHelp(){
        System.out.println(" Wait for lock file to be deleted on remote hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" wait-for-lock.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -ln   || --lockName       Lock file name relative to remote work directory.");
        System.out.println();
    }

    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getHostList(args.getClientHosts(), true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {

    }

    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total, String consID) {
        SSHCmdWorker worker = new SSHCmdWorker(args);

        String rmtLockName = String.format("%s/%s", rmtHome, args.getLockName());

        println(String.format("Waiting for lock file %s on the host %s",args.getLockName(), host));

        try {
            while (worker.exists(host, rmtLockName))
                sleep(3000L);
        }
        catch (Exception e){
            LOG.error(String.format("Failed to check lock file on the host %s", host), e);
        }

        println(String.format("Lock file %s is not found on the host %s",args.getLockName(), host));
    }
}

