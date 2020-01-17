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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.getHostList;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printw;

/**
 * Created by oostanin on 21.12.17.
 */
public class PrepareWorker extends AbstractWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(PrepareWorker.class.getName());

    public static final String CFG_BACKUPS_PLACEHOLDER = "\\{\\{BACKUPS.*\\}\\}";
    public static final String CFG_PART_LOSS_POLICY_PLACEHOLDER = "\\{\\{PART_LOSS_POLICY\\}\\}";

    private static final String TMPL_HOSTS_NAME = "hosts.xml";

    private static final String TMPL_HOSTS_ZK_NAME = "hosts-zk.xml";

    /** */
    private static final String DEF_PORT_RANGE = "47500..47509";

    private static final String DEF_ZK_PORT_RANGE = "2181";

    /** */
    private static final String DEF_SERV_CFG_PATH = "config/cluster/pds-inmem-remote-server-config.xml";
    /** */
    private static final String DEF_CLIENT_CFG_PATH = "config/cluster/pds-inmem-remote-client-config.xml";

    /** */
    public static void main(String[] args) {
        if(args.length == 0){
            printer("You can't run this script without any parameters.");

            System.exit(1);
        }

        PocTesterUtils.delete(Paths.get(System.getProperty("pocTesterHome"), "config", "common.properties")
            .toString());

        PocTesterArguments args0 = new PocTesterArguments();
        args0.setArgsFromCmdLine(args);

        PrepareWorker worker = new PrepareWorker();

        if(args0.isHelp()) {
            commonHelp();

            worker.printHelp();
        }

        worker.prepare(args0);
    }

    /** */
    protected void printHelp(){
        System.out.println(" Creates common property file 'config"+ File.separator + "common.properties'. Creates configuration files" +
                " for starting nodes on remote host. Creates 'prepared.zip' file to upload on remote hosts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" prepare.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        System.out.println();
        System.out.println(" -s    || --serverHosts     <comma separated list of server node IP addresses e.g. 172.1.0.1,172.1.0.2>");
        System.out.println(" -c    || --clientHosts     <comma separated list of client node IP addresses e.g. 172.1.0.1,172.1.0.2>");
        System.out.println(" -z    || --zooKeeperHosts  <comma separated list of zookeeper node IP addresses e.g. 172.1.0.1,172.1.0.2>");
        System.out.println(" -zp   || --zooKeeperPath   <path to zookeeper>");
        System.out.println(" -wd   || --remoteWorkDir   <path to upload zip file and run poc-tester in>");
        System.out.println(" -zf   || --zipFile         <path to zip file to be uploaded on remote hosts>");
        System.out.println(" -u    || --user            <user name for ssh connection>");
        System.out.println(" -kp   || --keyPath         <path to key file for ssh connection>");
        System.out.println(" -pp   || --passPhrase      <path phrase for key>");
        System.out.println(" -jh   || --definedJavaHome <path to java home on remote hosts>");
        System.out.println(" -scfg || --serverCfg       <path to server config file> By default config/cluster/vmipfinder-remote-server-config.xml");
        System.out.println(" -ccfg || --clientCfg       <path to client config file> By default config/cluster/vmipfinder-remote-client-config.xml");
        System.out.println(" -ibc  || --importedBaseCfg <path to imported base config file> By default config/cluster/ignite-base-config.xml");
        System.out.println(" -b    || --backups         <number of backups to start node with>");
        System.out.println(" -jmx  || --jmxEnabled      <flag for enabling jmx>");
        System.out.println(" -fr   || --flightRec       <flight record options e.g. 60:120 (pause:duration)>");
        System.out.println(" -wp   || --walPath         <defined WAL directory>");
        System.out.println(" -wa   || --walArch         <defined WAL archive director>");
        System.out.println();
    }

    /** */
    private void prepare(PocTesterArguments args) {
        if(args.getServerHosts() == null){
            LOG.error("Server host list is not defined!");

            throw new IllegalArgumentException("Server host list is not defined!");
        }

        Properties prop = new Properties();

        List<String> resLines = new ArrayList<>();

        if (args.getKeyPath() != null)
            prop.setProperty("SSH_KEY_PATH", args.getKeyPath());

        if (!args.getPassPhrase().equals(""))
            prop.setProperty("SSH_KEY_PASSPHRASE", args.getPassPhrase());

        prop.setProperty("ZIP_FILE", String.format("%s%sprepared.zip", this.locHome, File.separator));

        prop.setProperty("IMPORTED_BASE_CFG", args.getImportedBaseCfg());

        resLines.add("");
        resLines.add("# Username for ssh connection. Can be overwritten with -u option.");

        if (args.getUser() != null)
            resLines.add("USER=" + args.getUser());
        else
            resLines.add("USER=" + System.getProperty("user.name"));

        resLines.add("");
        resLines.add("# Remote work directory path. Can be overwritten with -wd option.");

        if (args.getRemoteWorkDir() != null) {
            resLines.add("REMOTE_WORK_DIR=" + args.getRemoteWorkDir());

            rmtHome = args.getRemoteWorkDir();
        }
        else {
            printw("Remote work directory is not set. Will use local home directory path for remote work.");

            resLines.add("REMOTE_WORK_DIR=" + locHome);

            rmtHome = locHome;
        }

        String commonPropPath = Paths.get(locHome, "config", "common.properties").toString();

        String servCfgName = args.getServerCfg() != null ? args.getServerCfg() :
                DEF_SERV_CFG_PATH;
        String servCfgPathRemote = String.format("%s/%s", rmtHome, servCfgName);

        String clientCfgName = args.getClientCfg() != null ? args.getClientCfg() :
                DEF_CLIENT_CFG_PATH;
        String clientCfgPathRemote = String.format("%s/%s", rmtHome, clientCfgName);

        resLines.add("");
        resLines.add("# Comma separated list of ip addresses to start servers. Can be overwritten with -s option.");
        resLines.add("SERVER_HOSTS=" + args.getServerHosts());

        resLines.add("");
        resLines.add("# Path to server config file on remote host. Can be overwritten with -scfg option.");
        resLines.add("SERVER_CONFIG_FILE=" + servCfgPathRemote);

        resLines.add("");
        resLines.add("# Comma separated list of ip addresses to start clients. Can be overwritten with -c option.");
        resLines.add("CLIENT_HOSTS=" + args.getClientHosts());

        resLines.add("");
        resLines.add("# Path to client config file on remote host. Can be overwritten with -ccfg option.");
        resLines.add("CLIENT_CONFIG_FILE=" + clientCfgPathRemote);

        if(args.getZooKeeperHosts() != null) {
            resLines.add("");
            resLines.add("# Comma separated list of ip addresses to start zookeeper. Can be overwritten with -zh option.");
            resLines.add("ZOOKEEPER_HOSTS=" + args.getZooKeeperHosts());
        }

        if(args.getZooKeeperPath() != null) {
            resLines.add("");
            resLines.add("# Path to zookeeper. Can be overwritten with -zp option.");
            resLines.add("ZOOKEEPER_PATH=" + args.getZooKeeperPath());
        }

        if (args.getBackups() != null) {
            resLines.add("");
            resLines.add("# A sequence backups to start node with. Exact number will be chosen randomly. " +
                    "Can be overwritten with -b option.");
            resLines.add("BACKUPS=" + args.getBackups());
        }

        if (args.getPartitionLossPolicy() != null) {
            resLines.add("");
            resLines.add("# Partition loss policy. Can be overwritten with -plp option.");
            resLines.add("PARTITION_LOSS_POLICY=" + args.getPartitionLossPolicy());
        }

        if (args.getDefinedJavaHome() != null) {
            resLines.add("");
            resLines.add("# Path on remote hosts from which java will be launched. Can be overwritten with -jh option.");
            resLines.add("DEFINED_JAVA_HOME=" + args.getDefinedJavaHome());
        }

        if (args.isJmxEnabled()) {
            resLines.add("");
            resLines.add("# Flag to enable remote jmx connection to ignite node jvm.");
            resLines.add("JMX_ENABLED=true");
        }

        if (args.getFlightRec() != null) {
            resLines.add("");
            resLines.add("# Java flight record options: <delay:duration>. Can be overwritten with -fr option.");
            resLines.add("JFR_OPTS=" + args.getFlightRec());
        }

        if (args.getStoragePath() != null) {
            resLines.add("");
            resLines.add("# Defined data storage path. Can be overwritten with -stp option.");
            resLines.add("STORAGE_PATH=" + args.getStoragePath());
        }

        if (args.getWalPath() != null) {
            resLines.add("");
            resLines.add("# Defined WAL path. Can be overwritten with -wp option.");
            resLines.add("WAL_PATH=" + args.getWalPath());
        }

        if (args.getWalArch() != null) {
            resLines.add("");
            resLines.add("# Defined WAL archive path. Can be overwritten with -wa option.");
            resLines.add("WAL_ARCHIVE_PATH=" + args.getWalArch());
        }

        if (args.getZooConnStr() != null) {
            resLines.add("");
            resLines.add("# Zookeeper connection string. Can be overwritten with -zk option.");
            resLines.add("ZOOKEEPER_CONNECTION_STRING=" + args.getZooConnStr());
        }

        if (args.isNoConfigCheck()) {
            resLines.add("");
            resLines.add("# Flag for no checking local config files.");
            resLines.add("NO_CONFIG_CHECK=true");
        }

        OutputStream output = null;

        PocTesterUtils.delete(commonPropPath);

        String propCom = " Common property file. \n First part of the file defines ssh key path and zip file path.";

        try {
            output = new FileOutputStream(commonPropPath);
            prop.store(output, propCom);
        }
        catch (IOException e) {
            LOG.error("Failed to save property file " + commonPropPath);
            e.printStackTrace();
        }

        try {
            PocTesterUtils.updateFile(Paths.get(commonPropPath), resLines);
        }
        catch (IOException e) {
            LOG.error("Failed to save property file " + commonPropPath);
            e.printStackTrace();
        }

        ////////////////////
        // Render hosts and caches templates
        RemoteConfigEditor editor = new RemoteConfigEditor(args);

        String portRange = args.getPortRange() == null ? DEF_PORT_RANGE : args.getPortRange();
        String zkPortRange = DEF_ZK_PORT_RANGE;

        try {
            editor.insertIpAddresses(
                    String.format("%s/config/cluster/templates/%s", locHome, TMPL_HOSTS_NAME),
                    String.format("%s/config/cluster/prepared-%s", locHome, TMPL_HOSTS_NAME),
                    PocTesterUtils.getHostList(args.getServerHosts(), true),
                    portRange
            );

            if (args.getZooKeeperHosts() != null) {
                editor.insertIpAddresses(
                        String.format("%s/config/cluster/templates/%s", locHome, TMPL_HOSTS_ZK_NAME),
                        String.format("%s/config/cluster/prepared-%s", locHome, TMPL_HOSTS_ZK_NAME),
                        getHostList(args.getZooKeeperHosts(), true),
                        zkPortRange
                );
                editor.prepareZkCfg();
            }
        }
        catch (IOException e) {
            LOG.error("Failed to edit config files.", e);
            System.exit(1);
        }

        ////////////////////
        // Save properties for cache config
        Path cachePropsPath = Paths.get(locHome, "config/cluster/caches.properties");

        if (cachePropsPath.toFile().exists()) {
            LOG.warn("Found properties file for caches. Will not overwrite");
        }
        else {
            Properties cacheProps = new Properties();

            cacheProps.setProperty("cache.backups.0", args.getBackupsNext());
            cacheProps.setProperty("cache.backups.1", args.getBackupsNext());
            cacheProps.setProperty("cache.backups.2", args.getBackupsNext());
            cacheProps.setProperty("cache.partitionLossPolicy", args.getPartitionLossPolicy());
            cacheProps.setProperty("cache.atomicityMode.0", args.getAtomicityModeNext());
            cacheProps.setProperty("cache.atomicityMode.1", args.getAtomicityModeNext());
            cacheProps.setProperty("cache.atomicityMode.2", args.getAtomicityModeNext());
            cacheProps.setProperty("cache.dataRegionName.0", args.getDataRegionNameModeNext());
            cacheProps.setProperty("cache.dataRegionName.1", args.getDataRegionNameModeNext());

            try {
                cacheProps.store(new FileOutputStream(cachePropsPath.toString()), "Properties used by caches config");
            } catch (IOException e) {
                LOG.error("Failed to save cache properties", e);
                System.exit(1);
            }
        }

        ////////////////////
        // Create a POC Tester artifact
        String dstZipPath = Paths.get(locHome, "prepared.zip").toString();

        List<String> filesToZip = new ArrayList<>();
        List<String> foldersToZip = Arrays.asList("bin", "config", "libs", "poc-tester-libs");

        try {
            PocTesterUtils.createZip(this.locHome, dstZipPath, filesToZip, foldersToZip);
        }
        catch (IOException e) {
            LOG.error("Failed to create zip file.");
            e.printStackTrace();
        }
    }

}