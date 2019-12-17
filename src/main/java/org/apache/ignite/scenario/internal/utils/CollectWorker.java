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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.cleanDirectory;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.hms;

/**
 * Class for collecting data from remote hosts.
 */
public class CollectWorker extends AbstractStartWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(CollectWorker.class.getName());

    private File logDir;

    public CollectWorker() {
        super();

        this.uniqHosts = true;
        this.startThreads = Runtime.getRuntime().availableProcessors();
    }

    /** */
    public static void main(String[] args) {
        CollectWorker worker = new CollectWorker();

        worker.work(args);

        try {
            worker.sort();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Print help.
     */
    protected void printHelp() {
        System.out.println(" Collects logs and results from remote hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" deploy.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -wd   || --remoteWorkDir   <path to upload zip file and run poc-tester in>");
        System.out.println();
    }

    @Override protected List<String> getHostList(PocTesterArguments args) {
        return PocTesterUtils.getAllHosts(args, uniqHosts);
    }

    /** {@inheritDoc} */
    @Override protected void beforeWork(PocTesterArguments args) {

    }

    /** */
    @Override public void start(PocTesterArguments args, String host, String dateTime, int cntr, int total, String consID) {
        final String tarName = dateTime + "-log.tar.gz";

        final SSHCmdWorker worker = new SSHCmdWorker(args);

        final String mkdirTempCmd = String.format("mkdir -p %s/log-temp", rmtHome);

        final String zipCmd = String.format("cp -r %s/log %s/log-temp; cd %s/log-temp/log; tar cfvz ../%s . ;",
                rmtHome, rmtHome, rmtHome, tarName);

        final String remoteZipPath = String.format("%s/log-temp/%s", rmtHome, tarName);

        final String rmtLogPath = String.format("%s/log", rmtHome);

        final String delCmd = String.format("rm -rf %s/log-temp", rmtHome);

        final String logDirName = System.getProperty("pocTesterHome") + File.separator + "log-" + dateTime;

        logDir = new File(logDirName);

        if (!logDir.exists())
            logDir.mkdirs();

        System.out.println(hms() + "Collecting data from the host " + host);

        String dstPath = logDirName + File.separator + host + "-" + tarName;

        try {
            if (!worker.exists(host, rmtLogPath)) {
                System.out.println(hms() + "No log directory on the host " + host);

                return;
            }

            worker.runCmd(host, mkdirTempCmd);

            worker.runCmd(host, zipCmd);

            worker.get(host, remoteZipPath, dstPath);

            worker.runCmd(host, delCmd);

            String unzippedDir = logDirName + File.separator + host;

            File tar = new File(dstPath);

            if (!tar.exists()) {
                LOG.error(String.format("%s file does not exists", dstPath));

                return;
            }

            try {
                Archiver archiver = ArchiverFactory.createArchiver("tar", "gz");

                archiver.extract(tar, new File(unzippedDir));

                tar.delete();
            }
            catch (Exception e) {
                LOG.error(String.format("Failed to extract files from archive %s", tar), e);
            }
        }
        catch (Exception e){
            LOG.error(String.format("Failed to collect files from the host %s", host), e);

        }
    }

    protected void sort() throws IOException {
        System.out.println(hms() + "Sorting data.");

        File[] hostDirs = logDir.listFiles();

        String serverDirPath = logDir + File.separator + "servers";
        String clientDirPath = logDir + File.separator + "clients";
        String zkDirPath = logDir + File.separator + "zookeeper";
        String statDirPath = logDir + File.separator + "stats";
        String checkDirPath = logDir + File.separator + "check";

        File serverDir = new File(serverDirPath);
        File clientDir = new File(clientDirPath);
        File zkDir = new File(zkDirPath);
        File statDir = new File(statDirPath);
        File checkDir = new File(checkDirPath);

        for (File hostDir : hostDirs) {
            if (!hostDir.isDirectory()) {
                LOG.error(String.format("%s is not a directory.", hostDir));
                continue;
            }

            String host = hostDir.getName();

            File[] serverDirs = PocTesterUtils.getSubDirs(hostDir.toPath(), "server");

            for (File hostServerDir : serverDirs) {
                if (!serverDir.exists())
                    serverDir.mkdir();

                File hostSubDir = new File(serverDirPath + File.separator + host);

                if (!hostSubDir.exists())
                    hostSubDir.mkdir();

                File[] serverDirContent = hostServerDir.listFiles();

                for (File contentFile : serverDirContent) {
                    String contentFileName = contentFile.getName();

                    File dst = new File(hostSubDir + File.separator + contentFileName);

                    Files.move(contentFile.toPath(), dst.toPath());
                }

                cleanDirectory(hostServerDir.getAbsolutePath());
                hostServerDir.delete();
            }

            File[] clientDirs = PocTesterUtils.getSubDirs(hostDir.toPath(), "task");

            for (File hostClientDir : clientDirs) {
                if (!clientDir.exists())
                    clientDir.mkdir();

                File hostSubDir = new File(clientDirPath + File.separator + host);

                if (!hostSubDir.exists())
                    hostSubDir.mkdir();

                String clientDirName = hostClientDir.getName();

                File dst = new File(hostSubDir + File.separator + clientDirName);

                Files.move(hostClientDir.toPath(), dst.toPath());
            }

            File[] zkDirs = PocTesterUtils.getSubDirs(hostDir.toPath(), "zookeeper");

            for (File hostZkDir : zkDirs) {
                if (!zkDir.exists())
                    zkDir.mkdir();

                File hostSubDir = new File(zkDirPath + File.separator + host);

                if (!hostSubDir.exists())
                    hostSubDir.mkdir();

                File[] zkDirContent = hostZkDir.listFiles();

                for (File contentFile : zkDirContent) {
                    String contentFileName = contentFile.getName();

                    File dst = new File(hostSubDir + File.separator + contentFileName);

                    Files.move(contentFile.toPath(), dst.toPath());
                }

                cleanDirectory(hostZkDir.getAbsolutePath());
                hostZkDir.delete();
            }

            File[] statDirs = PocTesterUtils.getSubDirs(hostDir.toPath(), "stats");

            for (File hostStatDir : statDirs) {
                if (!statDir.exists())
                    statDir.mkdir();

                File hostSubDir = new File(statDirPath + File.separator + host);

                if (!hostSubDir.exists())
                    hostSubDir.mkdir();

                String statDirName = hostStatDir.getName();

//                File dst = new File(hostSubDir + File.separator + statDirName);
//                File dst = hostSubDir;

//                LOG.info(hostStatDir.getAbsolutePath());
//                LOG.info(hostSubDir.getAbsolutePath());

                File[] statSubDirs = hostStatDir.listFiles();

                if(statSubDirs == null){
                    LOG.error(String.format("Failed to get sub directories from directory %s", hostStatDir));

                    continue;
                }

                for(File statSubDir : statSubDirs)
                    Files.move(statSubDir.toPath(), Paths.get(hostSubDir.getAbsolutePath(), statSubDir.getName()));

            }

            File[] checkDirs = PocTesterUtils.getSubDirs(hostDir.toPath(), "check");

            for (File hostCheckDir : checkDirs) {
                if (!checkDir.exists())
                    checkDir.mkdir();

                File hostSubDir = new File(checkDirPath + File.separator + host);

                if (!hostSubDir.exists())
                    hostSubDir.mkdir();

                String checkDirName = hostCheckDir.getName();

                File dst = new File(hostSubDir + File.separator + checkDirName);

                Files.move(hostCheckDir.toPath(), dst.toPath());
            }

            cleanDirectory(hostDir.getAbsolutePath());
            hostDir.delete();
        }
    }
}
