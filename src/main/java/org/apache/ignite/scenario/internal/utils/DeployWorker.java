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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.ignite.scenario.internal.PocTesterArguments;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.isLocal;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;

/**
 * Class for deploying files to remote hosts.
 */
public class DeployWorker extends AbstractStartWorker {

    public DeployWorker() {
        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    /** */
    public static void main(String[] args) {
        new DeployWorker().work(args);
    }

    /**
     * Print help.
     */
    protected void printHelp() {
        System.out.println(" Deploys zip file to remote hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" deploy.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -wd   || --remoteWorkDir   <path to upload zip file and run poc-tester in>");
        System.out.println(" -zf   || --zipFile         <relative path to zip file to be uploaded on remote hosts>");
        System.out.println();
    }

    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getPocHosts(args, uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {
        println(String.format("Uploading %s to remote hosts:", args.getZipFile()));
    }

    /** */
    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total,
        String consID) {

//        final String zipPath = Paths.get(System.getProperty("pocTesterHome"), "prepared.zip").toString();
        final String zipPath = args.getZipFile();

        if (!new File(zipPath).exists()) {
            printer(String.format("File %s is not found. Please run prepare.sh or zip poc-tester directories in " +
                "'prepared.zip file'", zipPath));

            return;
        }

        final String rmtZipName = new File(zipPath).getName();

        final String rmtZipPath = String.format("%s/%s", rmtHome, rmtZipName );

        final String delCmd = String.format("rm -rf %s", rmtHome);

        final String delZipCmd = String.format("rm -rf %s", rmtZipPath);

        final String mkdirCmd = String.format("mkdir -p %s", rmtHome);

        final String chmodCmd = String.format("chmod +x %s/bin/*", rmtHome);

        final String chmodIncludeCmd = String.format("chmod +x %s/bin/include/*", rmtHome);

        final String unzipCmd = String.format("unzip -q %s -d %s", rmtZipPath, rmtHome);

        try {
            if (isLocal(InetAddress.getByName(host)) && locHome.equals(rmtHome)) {
                println(String.format("%s is local machine address. Will not deploy.",  host));

                return;
            }
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }

        SSHCmdWorker worker = new SSHCmdWorker(args);

        try {
            worker.runCmd(host, delCmd);

            worker.runCmd(host, mkdirCmd);

            worker.put(host, zipPath, rmtZipPath);

            worker.runCmd(host, unzipCmd);

            worker.runCmd(host, chmodCmd);

            worker.runCmd(host, chmodIncludeCmd);

            worker.runCmd(host, delZipCmd);

            println(String.format("%s - done.", host));

        }
        catch (Exception e){
            LOG.error(String.format("Failed to deploy files on the host %s", host), e);
        }

    }
}

