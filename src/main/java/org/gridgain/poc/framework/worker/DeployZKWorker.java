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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;

import static org.gridgain.poc.framework.utils.PocTesterUtils.isLocal;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;
import static org.gridgain.poc.framework.utils.PocTesterUtils.println;

/**
 * Class for deploying files to remote hosts.
 */
public class DeployZKWorker extends AbstractStartWorker {

    public DeployZKWorker() {
        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    /** */
    public static void main(String[] args) {
        new DeployZKWorker().work(args);
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
        System.out.println(" -zp   || --zooKeeperPath   <path to zookeeper>");
        System.out.println();
    }

    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getZooKeeperHosts(args, uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {
        println(String.format("Uploading %s to remote hosts:", args.getZooKeeperPath()));
    }

    /** */
    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total,
        String consID) {

//        final String zipPath = Paths.get(System.getProperty("pocTesterHome"), "prepared.zip").toString();
        final String zkPath = args.getZooKeeperPath();

        if (!new File(zkPath).exists()) {
            printer(String.format("File %s is not found.", zkPath));

            return;
        }

        final String rmtZKName = new File(zkPath).getName();

        final String rmtZKDir = String.format("%s/zookeeper", rmtHome);

        final String rmtZKPath = String.format("%s/%s", rmtZKDir, rmtZKName );

        final String delCmd = String.format("rm -rf %s", rmtZKDir);

        final String delZipCmd = String.format("rm -rf %s", rmtZKPath);

        final String mkdirCmd = String.format("mkdir -p %s", rmtZKDir);

        final String chmodCmd = String.format("chmod +x %s/bin/*", rmtHome);

        final String chmodIncludeCmd = String.format("chmod +x %s/bin/include/*", rmtHome);

        final String unzipCmd = String.format("cd %s; tar -xvzf %s", rmtZKDir, rmtZKPath);

        try {
            if (isLocal(InetAddress.getByName(host)) && locHome.equals(rmtHome)) {
                println(String.format("%s is local machine address. Will not deploy.",  host));

                return;
            }
        }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }

        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        try {
            worker.runCmd(host, delCmd);

            worker.runCmd(host, mkdirCmd);

            worker.put(host, zkPath, rmtZKPath);

            worker.runCmd(host, unzipCmd);

//            worker.runCmd(host, chmodCmd);

//            worker.runCmd(host, chmodIncludeCmd);

            worker.runCmd(host, delZipCmd);

            String zkDir = worker.runCmd(host, String.format("cd %s; ls", rmtZKDir)).get(0);

            String moveCmd = String.format("mv %s/%s/* %s", rmtZKDir, zkDir, rmtZKDir);

            worker.runCmd(host, moveCmd);

            String rmZkDirCmd = String.format("rm -rf %s/%s", rmtZKDir, zkDir);

            worker.runCmd(host, rmZkDirCmd);

            String zkDataDir = String.format("%s/zookeeper/data", args.getRemoteWorkDir());
            String zkLogDir = String.format("%s/log/zookeeper", args.getRemoteWorkDir());

            worker.runCmd(host, "mkdir -p " + zkDataDir);
            worker.runCmd(host, "mkdir -p " + zkLogDir);


            String zkCfgSrc = String.format("%s/config/cluster/zookeeper/prepared-zoo.cfg", locHome);
            String javaEnvSrc = String.format("%s/config/cluster/zookeeper/prepared-java.env", locHome);

            String zkCfgDest = String.format("%s/conf/zoo.cfg", rmtZKDir);
            String javaEnvDest = String.format("%s/conf/java.env", rmtZKDir);

            worker.put(host, zkCfgSrc, zkCfgDest);
            worker.put(host, javaEnvSrc, javaEnvDest);

            println(String.format("%s - done.", host));

        }
        catch (Exception e){
            LOG.error(String.format("Failed to deploy files on the host %s", host), e);
        }



    }
}

