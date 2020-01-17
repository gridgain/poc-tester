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

package org.gridgain.poc.framework.worker.task;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.gridgain.poc.framework.worker.IpWorker;
import org.gridgain.poc.framework.utils.PocTesterUtils;

/**
 * Input arguments.
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
public class PocTesterArguments implements Cloneable {
    private static final String DFLT_START_PROPS_REL_PATH = "config/common.properties";
    private static final String DFLT_IMPORTED_BASE_CFG = "config/cluster/caches/caches-base.xml";
    private static final String DFLT_BACKUPS = "0";
    private static final String DFLT_PART_LOSS_POLICY = "READ_WRITE_SAFE";
    private static final String DFLT_ATOMICITY_MODES = "TRANSACTIONAL";
    private static final String DFLT_DATA_REGION_NAMES = "inMemoryConfiguration:persistenceConfiguration";

    /** */
    @Parameter(names = {"-h", "--help"}, description = "Print help flag. ")
    private boolean help;

    /** */
    @Parameter(names = {"-ccfg", "--clientCfg"}, description = "Client config")
    private String clientCfg;

    /** */
    @Parameter(names = {"-scfg", "--serverCfg"}, description = "Server config")
    private String serverCfg;

    /** */
    @Parameter(names = {"-ibc", "--importedBaseCfg"}, description = "Imported base config")
    private String importedBaseCfg = DFLT_IMPORTED_BASE_CFG;

    /** */
    @Parameter(names = {"-tp", "--taskProperties"}, description = "Task properties")
    private String taskProperties;

    /** */
    @Parameter(names = {"-s", "--serverHosts"}, description = "Server hosts")
    private String serverHosts;

    /** */
    @Parameter(names = {"-c", "--clientHosts"}, description = "Client hosts")
    private String clientHosts;

    /** */
    @Parameter(names = {"-z", "--zooKeeperHosts"}, description = "Zookeeper hosts")
    private String zooKeeperHosts;

    /** */
    @Parameter(names = {"-zp", "--zooKeeperPath"}, description = "Zookeeper path.")
    private String zooKeeperPath;

    /** */
    @Parameter(names = {"-pr", "--portRange"}, description = "Port range")
    private String portRange;

    /** */
    @Parameter(names = {"-wd", "--remoteWorkDir"}, description = "Work directory")
    private String remoteWorkDir;

    /** */
    @Parameter(names = {"-hd", "--homeDir"}, description = "Home directory")
    private String homeDir;

    /** */
    @Parameter(names = {"-stp", "--storagePath"}, description = "Data storage path")
    private String storagePath;

    /** */
    @Parameter(names = {"-wp", "--walPath"}, description = "WAL path")
    private String walPath;

    /** */
    @Parameter(names = {"-wa", "--walArch"}, description = "WAL archive path")
    private String walArch;

    /** */
    @Parameter(names = {"-ca", "--cleanAll"}, description = "Flag for cleaning entire remote work directory.")
    private boolean cleanAll;

    /** */
    @Parameter(names = {"-rb", "--removeBackups"}, description = "Flag for removing backups.")
    private boolean removeBackups;

    /** */
    @Parameter(names = {"-zk", "--zooConnStr"}, description = "Zookeeper connection string")
    private String zooConnStr;

    /** */
    @Parameter(names = {"-nc", "--noConfigCheck"}, description = "Flag for no local config checking.")
    private boolean noConfigCheck;

    /** */
    @Parameter(names = {"-sp", "--startProps"}, description = "Common property file for starting tasks")
    private String startProps = DFLT_START_PROPS_REL_PATH;

    /** */
    @Parameter(names = {"-zf", "--zipFile"}, description = "Zip file.")
    private String zipFile;

    /** */
    @Parameter(names = {"-jh", "--definedJavaHome"}, description = "Defined java home for remote hosts.")
    private String definedJavaHome;

    /** */
    @Parameter(names = {"-id", "--nodeID"}, description = "Node consistent ID.")
    private String nodeID;

    /** */
    @Parameter(names = {"-fs", "--fastStart"}, description = "Flag for fast start.")
    private Integer fastStart;

    /** */
    @Parameter(names = {"-st", "--startThreads"}, description = "Number of start threads.")
    private Integer startThreads;

    /** */
    @Parameter(names = {"-cntr", "--nodeCntr"}, description = "Node counter.")
    private int nodeCntr;

    /** */
    @Parameter(names = {"-ttl", "--totalNodesNum"}, description = "Total number of nodes.")
    private int totalNodesNum;

    /** A sequence of possible backups for a cache. E.g. "1:2:3". Exact number will be chosen randomly. */
    @Parameter(names = {"-b", "--backups"}, description = "Number of backups.")
    private String backups = DFLT_BACKUPS;

    /** */
    @Parameter(names = {"-plp", "--partitionLossPolicy"}, description = "Partition loss policy.")
    private String partitionLossPolicy = DFLT_PART_LOSS_POLICY;

    @Parameter(names = {"--atomicityModes"}, description = "Atomicity modes for main caches used in a scenario (colon-separated).")
    private String atomicityModes = DFLT_ATOMICITY_MODES;

    @Parameter(names = {"--dataRegionNames"}, description = "Data region names from main config (colon-separated).")
    private String dataRegionNames = DFLT_DATA_REGION_NAMES;

    /** */
    @Parameter(names = {"-stop", "--stopFlag"}, description = "Defined java home for remote hosts.")
    private boolean stopFlag;

    /** */
    @Parameter(names = {"-a", "--allJava"}, description = "Flag for killing all java processes on remote hosts")
    private boolean killAllJava;

    /** */
    @Parameter(names = {"-km", "--killMode"}, description = "Kill mode.")
    private String killMode = "-9";

    /** */
    @Parameter(names = {"-su", "--sudo"}, description = "Flag for running commands as a superuser")
    private boolean sudo;

    /** */
    @Parameter(names = {"-u", "--user"}, description = "User")
    private String user;

    /** */
    @Parameter(names = {"-kp", "--keyPath"}, description = "Identity key path")
    private String keyPath;

    /** */
    @Parameter(names = {"-pp", "--passPhrase"}, description = "Identity key passphrase")
    private String passPhrase = "";

    /** */
    @Parameter(names = {"-tr", "--timeToRun"}, description = "Time to run.")
    private int timeToRun;

    /** */
    @Parameter(names = {"-ti", "--timeInterval"}, description = "Time interval.")
    private int timeInterval = 30;

    /** */
    @Parameter(names = {"-du", "--discUsage"}, description = "Flag for dumping disc usage statistic.")
    private boolean discUsage;

    /** */
    @Parameter(names = {"-td", "--threadDump"}, description = "Flag for taking thread dumps.")
    private boolean threadDump;

    /** */
    @Parameter(names = {"-dst", "--dstat"}, description = "Option for start/stop dstat.")
    private String dstat;

    /** */
    @Parameter(names = {"-pst", "--processStats"}, description = "Option for collecting process statistics.")
    private boolean processStats;

    /** */
    @Parameter(names = {"-gl", "--grepLines"}, description = "Comma separated list of lines to grep from remote log files.")
    private String grepLines;

    /** */
    @Parameter(names = {"-jmx", "--jmxEnabled"}, description = "Flag for enabling remote jmx connection.")
    private boolean jmxEnabled;

    /** */
    @Parameter(names = {"-fr", "--flightRec"}, description = "Flight record options.")
    private String flightRec;

    /** */
    @Parameter(names = {"-re", "--report"}, description = "Make report along with collect flag.")
    private boolean report;

    /** */
    @Parameter(names = {"-ln", "--lockName"}, description = "Lock file name.")
    private String lockName;

    /** */
    @Parameter(names = {"-rd", "--resDir"}, description = "Result directory.")
    private String resDir;

    /** */
    @Parameter(names = {"-lac", "--logAnalyzerConfig"}, description = "Config file path")
    private String logAnalyzerConfig = "config/log-analyzer-cfg/log-analyzer-context.yaml";

    /** */
    @Parameter(names = {"-idp", "--ideaDebugPath"}, description = "Path to project for IDE debug.")
    private String ideaDebugPath;

    /** */
    @Parameter(names = {"-shft", "--shiftNum"}, description = "Path to project for IDE debug.")
    private int shiftNum;

    /** */
    @Parameter(names = {"-back", "--shiftBack"}, description = "Flag for restoring shifted work directory.")
    private boolean shiftBack;

    private List<String> backupsList = new ArrayList<>();
    
    private List<String> atomicityModeList = new ArrayList<>();

    private List<String> dataRegionNameList = new ArrayList<>();

    public boolean isHelp() {
        return help;
    }

    public String getClientCfg() {
        return clientCfg;
    }

    public void setClientCfg(String clientCfg) {
        this.clientCfg = clientCfg;
    }

    public String getServerCfg() {
        return serverCfg;
    }

    public void setServerCfg(String serverCfg) {
        this.serverCfg = serverCfg;
    }

    public String getImportedBaseCfg() {
        return importedBaseCfg;
    }

    public void setImportedBaseCfg(String importedBaseCfg) {
        this.importedBaseCfg = importedBaseCfg;
    }

    public String getTaskProperties() {
        return taskProperties;
    }

    public void setTaskProperties(String taskProperties) {
        this.taskProperties = taskProperties;
    }

    public String getServerHosts() {
        if (serverHosts != null && (serverHosts.contains("..") || serverHosts.contains("x")))
            serverHosts = new IpWorker().convertDots(serverHosts);

        return serverHosts;
    }

    public void setServerHosts(String serverHosts) {
        this.serverHosts = serverHosts;
    }
    
    public String getClientHosts() {
        if (clientHosts != null && (clientHosts.contains("..") || clientHosts.contains("x")))
            clientHosts = new IpWorker().convertDots(clientHosts);

        return clientHosts;
    }

    public String getZooKeeperHosts() {
        if (zooKeeperHosts != null && (zooKeeperHosts.contains("..") || zooKeeperHosts.contains("x")))
            zooKeeperHosts = new IpWorker().convertDots(zooKeeperHosts);

        return zooKeeperHosts;
    }

    public void setZooKeeperHosts(String zooKeeperHosts) {
        this.zooKeeperHosts = zooKeeperHosts;
    }

    public String getZooKeeperPath() {
        return zooKeeperPath;
    }

    public void setZooKeeperPath(String zooKeeperPath) {
        this.zooKeeperPath = zooKeeperPath;
    }

    public void setClientHosts(String clientHosts) {
        this.clientHosts = clientHosts;
    }

    public String getPortRange() {
        return portRange;
    }

    public void setPortRange(String portRange) {
        this.portRange = portRange;
    }

    public String getRemoteWorkDir() {
        return remoteWorkDir;
    }

    public void setRemoteWorkDir(String remoteWorkDir) {
        this.remoteWorkDir = remoteWorkDir;
    }

    public String getHomeDir() {
        return homeDir;
    }

    public void setHomeDir(String homeDir) {
        this.homeDir = homeDir;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public void setStoragePath(String storagePath) {
        this.storagePath = storagePath;
    }

    public String getWalPath() {
        return walPath;
    }

    public void setWalPath(String walPath) {
        this.walPath = walPath;
    }

    public String getWalArch() {
        return walArch;
    }

    public void setWalArch(String walArch) {
        this.walArch = walArch;
    }

    public boolean isCleanAll() {
        return cleanAll;
    }

    public void setCleanAll(boolean cleanAll) {
        this.cleanAll = cleanAll;
    }

    public boolean isRemoveBackups() {
        return removeBackups;
    }

    public String getZooConnStr() {
        return zooConnStr;
    }

    public void setZooConnStr(String zooConnStr) {
        this.zooConnStr = zooConnStr;
    }

    public boolean isNoConfigCheck() {
        return noConfigCheck;
    }

    public void setNoConfigCheck(boolean noConfigCheck) {
        this.noConfigCheck = noConfigCheck;
    }

    public String getStartProps() {
        return startProps;
    }

    public void setStartProps(String startProps) {
        this.startProps = startProps;
    }

    public String getZipFile() {
        return zipFile;
    }

    public void setZipFile(String zipFile) {
        this.zipFile = zipFile;
    }

    public String getDefinedJavaHome() {
        return definedJavaHome;
    }

    public void setDefinedJavaHome(String definedJavaHome) {
        this.definedJavaHome = definedJavaHome;
    }

    public boolean isStopFlag() {
        return stopFlag;
    }

    public void setStopFlag(boolean stopFlag) {
        this.stopFlag = stopFlag;
    }

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public Integer fastStart() {
        return fastStart;
    }

    public void setFastStart(Integer fastStart) {
        this.fastStart = fastStart;
    }

    public Integer getStartThreads() {
        return startThreads;
    }

    public void setStartThreads(Integer startThreads) {
        this.startThreads = startThreads;
    }

    public int getNodeCntr() {
        return nodeCntr;
    }

    public void setNodeCntr(int nodeCntr) {
        this.nodeCntr = nodeCntr;
    }

    public int getTotalNodesNum() {
        return totalNodesNum;
    }

    public String getBackups() {
        return backups;
    }

    public void setBackups(String backups) {
        this.backups = backups;
    }

    public String getPartitionLossPolicy() {
        return partitionLossPolicy;
    }

    public void setPartitionLossPolicy(String partitionLossPolicy) {
        this.partitionLossPolicy = partitionLossPolicy;
    }

    public String getAtomicityModes() {
        return atomicityModes;
    }

    public void setAtomicityModes(String atomicityModes) {
        this.atomicityModes = atomicityModes;
    }

    public String getDataRegionNames() {
        return dataRegionNames;
    }

    public void setDataRegionNames(String dataRegionNames) {
        this.dataRegionNames = dataRegionNames;
    }

    public void setTotalNodesNum(int totalNodesNum) {
        this.totalNodesNum = totalNodesNum;
    }

    public boolean isKillAllJava() {
        return killAllJava;
    }

    public void setKillAllJava(boolean killAllJava) {
        this.killAllJava = killAllJava;
    }

    public String getKillMode() {
        return killMode;
    }

    public void setKillMode(String killMode) {
        this.killMode = killMode;
    }

    public boolean isSudo() {
        return sudo;
    }

    public void setSudo(boolean sudo) {
        this.sudo = sudo;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getKeyPath() {
        return keyPath;
    }

    public void setKeyPath(String keyPath) {
        this.keyPath = keyPath;
    }

    public String getPassPhrase() {
        return passPhrase;
    }

    public void setPassPhrase(String passPhrase) {
        this.passPhrase = passPhrase;
    }

    public int getTimeToRun() {
        return timeToRun;
    }

    public void setTimeToRun(int timeToRun) {
        this.timeToRun = timeToRun;
    }

    public int getTimeInterval() {
        return timeInterval;
    }

    public void setTimeInterval(int timeInterval) {
        this.timeInterval = timeInterval;
    }

    public boolean isDiscUsage() {
        return discUsage;
    }

    public void setDiscUsage(boolean discUsage) {
        this.discUsage = discUsage;
    }

    public boolean isThreadDump() {
        return threadDump;
    }

    public void setThreadDump(boolean threadDump) {
        this.threadDump = threadDump;
    }

    public String getDstat() {
        return dstat;
    }

    public boolean procStat() {
        return processStats;
    }

    public void setProcessStats(boolean processStats) {
        this.processStats = processStats;
    }

    public String getGrepLines() {
        return grepLines;
    }

    public void setGrepLines(String grepLines) {
        this.grepLines = grepLines;
    }

    public boolean isJmxEnabled() {
        return jmxEnabled;
    }

    public void setJmxEnabled(boolean jmxEnabled) {
        this.jmxEnabled = jmxEnabled;
    }

    public String getFlightRec() {
        return flightRec;
    }

    public void setFlightRec(String flightRec) {
        this.flightRec = flightRec;
    }

    public boolean isReport() {
        return report;
    }

    public String getLockName() {
        return lockName;
    }

    public void setLockName(String lockName) {
        this.lockName = lockName;
    }

    public String getResDir() {
        return resDir;
    }

    public void setResDir(String resDir) {
        this.resDir = resDir;
    }

    public String getLogAnalyzerConfig() {
        return logAnalyzerConfig;
    }

    public void setLogAnalyzerConfig(String logAnalyzerConfig) {
        this.logAnalyzerConfig = logAnalyzerConfig;
    }

    public String getIdeaDebugPath() {
        return ideaDebugPath;
    }

    public int getShiftNum() {
        return shiftNum;
    }

    public void setShiftNum(int shiftNum) {
        this.shiftNum = shiftNum;
    }

    public boolean isShiftBack() {
        return shiftBack;
    }

    public void setShiftBack(boolean shiftBack) {
        this.shiftBack = shiftBack;
    }

    @Override protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public void setArgsFromCmdLine(String[] args) {
        PocTesterUtils.jcommander(args, this);
    }

    public void setArgsFromCmdLine(String[] args, String programName) {
        PocTesterUtils.jcommander(args, this, programName);
    }

    public void setArgsFromStartProps() {
        Properties prop = PocTesterUtils.loadProp(startProps);

        if (prop == null)
            return;

        // TODO: do not use setters/getters
        if (this.getServerHosts() == null && prop.getProperty("SERVER_HOSTS") != null)
            this.setServerHosts(prop.getProperty("SERVER_HOSTS"));

        if (this.getClientHosts() == null && prop.getProperty("CLIENT_HOSTS") != null)
            this.setClientHosts(prop.getProperty("CLIENT_HOSTS"));

        if (this.getZooKeeperHosts() == null && prop.getProperty("ZOOKEEPER_HOSTS") != null)
            this.setZooKeeperHosts(prop.getProperty("ZOOKEEPER_HOSTS"));

        if (this.getZooKeeperPath() == null && prop.getProperty("ZOOKEEPER_PATH") != null)
            this.setZooKeeperPath(prop.getProperty("ZOOKEEPER_PATH"));

        if (this.getClientHosts() == null && prop.getProperty("CLIENT_HOSTS") != null)
            this.setClientHosts(prop.getProperty("CLIENT_HOSTS"));

        if (this.getZipFile() == null && prop.getProperty("ZIP_FILE") != null)
            this.setZipFile(prop.getProperty("ZIP_FILE"));

        if (this.getServerCfg() == null && prop.getProperty("SERVER_CONFIG_FILE") != null)
            this.setServerCfg(prop.getProperty("SERVER_CONFIG_FILE"));

        if (this.getClientCfg() == null && prop.getProperty("CLIENT_CONFIG_FILE") != null)
            this.setClientCfg(prop.getProperty("CLIENT_CONFIG_FILE"));

        if (this.getImportedBaseCfg().equals(DFLT_IMPORTED_BASE_CFG) && prop.getProperty("IMPORTED_BASE_CFG") != null)
            this.setImportedBaseCfg(prop.getProperty("IMPORTED_BASE_CFG"));

        if (this.getRemoteWorkDir() == null && prop.getProperty("REMOTE_WORK_DIR") != null)
            this.setRemoteWorkDir(prop.getProperty("REMOTE_WORK_DIR"));

        if (this.getDefinedJavaHome() == null && prop.getProperty("DEFINED_JAVA_HOME") != null)
            this.setDefinedJavaHome(prop.getProperty("DEFINED_JAVA_HOME"));

        if (this.getKeyPath() == null && prop.getProperty("SSH_KEY_PATH") != null)
            this.setKeyPath(prop.getProperty("SSH_KEY_PATH"));

        if (this.getPassPhrase().equals("") && prop.getProperty("SSH_KEY_PASSPHRASE") != null)
            this.setPassPhrase(prop.getProperty("SSH_KEY_PASSPHRASE"));

        if (this.getUser() == null && prop.getProperty("USER") != null)
            this.setUser(prop.getProperty("USER"));

        if (this.getBackups().equals(DFLT_BACKUPS) && prop.getProperty("BACKUPS") != null)
            this.setBackups(prop.getProperty("BACKUPS"));

        if (this.getPartitionLossPolicy().equals(DFLT_PART_LOSS_POLICY) && prop.getProperty("PARTITION_LOSS_POLICY") != null)
            this.setPartitionLossPolicy(prop.getProperty("PARTITION_LOSS_POLICY"));

        if (!this.isJmxEnabled() && prop.getProperty("JMX_ENABLED") != null)
            this.setJmxEnabled(Boolean.valueOf(prop.getProperty("JMX_ENABLED")));

        if (this.getFlightRec() == null && prop.getProperty("JFR_OPTS") != null)
            this.setFlightRec(prop.getProperty("JFR_OPTS"));

        if (this.getStoragePath() == null && prop.getProperty("STORAGE_PATH") != null)
            this.setStoragePath(prop.getProperty("STORAGE_PATH"));

        if (this.getWalPath() == null && prop.getProperty("WAL_PATH") != null)
            this.setWalPath(prop.getProperty("WAL_PATH"));

        if (this.getWalArch() == null && prop.getProperty("WAL_ARCHIVE_PATH") != null)
            this.setWalArch(prop.getProperty("WAL_ARCHIVE_PATH"));

        if (this.getZooConnStr() == null && prop.getProperty("ZOOKEEPER_CONNECTION_STRING") != null)
            this.setZooConnStr(prop.getProperty("ZOOKEEPER_CONNECTION_STRING"));

        if (!this.isNoConfigCheck() && prop.getProperty("NO_CONFIG_CHECK") != null)
            this.setNoConfigCheck(Boolean.valueOf(prop.getProperty("NO_CONFIG_CHECK")));
    }

    public String getBackupsNext() {
        if (backupsList.size() == 0) {
            backupsList = new ArrayList<>(
                    Arrays.asList(backups.split(":"))
            );
            Collections.shuffle(backupsList);
        }

        return backupsList.remove(0);
    }
    
    public String getAtomicityModeNext() {
        if (atomicityModeList.size() == 0) {
            atomicityModeList = new ArrayList<>(
                    Arrays.asList(atomicityModes.split(":"))
            );
            Collections.shuffle(atomicityModeList);
        }

        return atomicityModeList.remove(0);
    }

    public String getDataRegionNameModeNext() {
        if (dataRegionNameList.size() == 0) {
            dataRegionNameList = new ArrayList<>(
                    Arrays.asList(dataRegionNames.split(":"))
            );
            Collections.shuffle(dataRegionNameList);
        }

        return dataRegionNameList.remove(0);
    }

    public PocTesterArguments cloneArgs(){
        try {
            return (PocTesterArguments) clone();
        }
        catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        return null;
    }
}
