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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;

import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;
import static org.gridgain.poc.framework.utils.PocTesterUtils.println;

/**
 * Clean worker.
 */
public class CleanWorker extends AbstractStartWorker {
    /**
     *  Constructor.
     */
    public CleanWorker() {
        super();

        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    public CleanWorker(PocTesterArguments args) {
        super();

        this.args = args;
        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();

        this.rmtHome = args.getRemoteWorkDir();
    }

    /**
     * Main.
     *
     * @param args Arguments.
     */
    public static void main(String[] args) {
        new CleanWorker().work(args);
    }

    /**
     * Print help.
     */
    @Override protected void printHelp() {
        System.out.println(" Clean Ignite work directory on remote hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" clean.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -wd   || --remoteWorkDir   <path to upload zip file and run poc-tester in>");
        System.out.println(" -rb   || --removeBackups   Flag indicating to remove backups also.");
        System.out.println(" -ca   || --cleanAll        <Flag for cleaning entire remote work directory>");
    }

    /** {@inheritDoc} */
    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getAllHosts(args, uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {

    }

    /** {@inheritDoc} */
    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total, String consID) {
        String igniteWorkDir = String.format("%s/work", rmtHome);

        if(args.isCleanAll())
            igniteWorkDir = rmtHome;

        String zkDataDir = String.format("%s/zookeeper/data", rmtHome);

        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        Collection<String> dirsToDel = new ArrayList<>();

        //Add to the list WAL, WAL-archive directories if those directories defined to be other than default.
        if (args.getWalPath() != null)
            dirsToDel.add(args.getWalPath());

        if (args.getWalArch() != null)
            dirsToDel.add(args.getWalArch());

        //Path to file containing backup suffix if there is backups.
        String backupSuffixPath = String.format("%s/backup-suffix.txt", rmtHome);

        String backupSuffix = null;

        boolean rmBackups = false;

        if (args.isRemoveBackups()) {
            try {
                if (worker.exists(host, backupSuffixPath)) {
                    List<String> res = worker.runCmd(host, String.format("cat %s", backupSuffixPath));

                    backupSuffix = res.get(0);

                    rmBackups = true;
                }
                else {
                    LOG.error(String.format("Failed to remove backups on the host %s. Cannot find suffix file %s",
                        host, backupSuffixPath));
                }
            }
            catch (Exception e) {
                LOG.error(String.format("Failed to remove backups on the host %s", host), e);
            }
        }

        if(rmBackups) {
            Collection<String> backupsDirToDel = new ArrayList<>();

            //Take name of directory to delete and add '-backup-<Date-Time>' for WAL WAL-archive
            // directories if those directories defined to be other than default, than add them to backup directories list.
            for (String dirToDelete : dirsToDel)
                backupsDirToDel.add(String.format("%s-backup-%s", dirToDelete, backupSuffix));

            //Add main backup directory.
            backupsDirToDel.add(String.format("%s-backup-%s", rmtHome, backupSuffix));

            //Add all backup directories to main list.
            dirsToDel.addAll(backupsDirToDel);
        }

        //Add Ignite work directory to main list.
        dirsToDel.add(igniteWorkDir);

        if(args.getZooKeeperHosts() != null)
            dirsToDel.add(zkDataDir);

        for (String dirToDelete : dirsToDel) {
            try {
                println(String.format("Deleting %s directory on the host %s", dirToDelete, host));

                worker.runCmd(host, String.format("rm -rf %s", dirToDelete));
            }
            catch (Exception e) {
                printer(String.format("Failed to delete directory %s on the host %s; Error message: %s",
                    dirToDelete, host, e.getMessage()));
            }
        }
    }

    public void cleanPds(String host, String consIdDirName) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String pdsDir = args.getStoragePath() == null ?
            String.format("%s/work/db/%s", rmtHome, consIdDirName):
            String.format("%s/%s", args.getStoragePath(), consIdDirName);

        String rmtCmd = String.format("rm -rf %s", pdsDir);

        worker.runCmd(host, rmtCmd);
    }

    public void cleanWAL(String host, String consIdDirName) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String walDir = args.getWalPath() == null ?
            String.format("%s/work/db/wal/%s", rmtHome, consIdDirName):
            String.format("%s/%s", args.getWalPath(), consIdDirName);

        String rmtCmd = String.format("rm -rf %s", walDir);

        worker.runCmd(host, rmtCmd);
    }

    public void cleanWALArch(String host, String consIdDirName) throws Exception {
        RemoteSshExecutor worker = new RemoteSshExecutor(args);

        String walArchDir = args.getWalArch() == null ?
            String.format("%s/work/db/wal/archive/%s", rmtHome, consIdDirName):
            String.format("%s/%s", args.getWalArch(), consIdDirName);

        String rmtCmd = String.format("rm -rf %s", walArchDir);

        worker.runCmd(host, rmtCmd);
    }
}
