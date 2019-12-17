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

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.utils.handlers.CommandExecutionResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;

public class SSHCmdWorker extends AbstractCmdWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(SSHCmdWorker.class.getName());

    /** */
    private static final int SSH_CHANNEL_TIMEOUT = 60_000;

    /**
     *
     * @param args
     */
    public SSHCmdWorker(PocTesterArguments args) {
        super(args);
    }

    public List<String> runCmd(String host, String cmd) throws Exception {
        List<String> res = new ArrayList<>();
        try {
            Session session = getSession(host);

            LOG.debug("Connected to the host: " + host);

            res = runRmtCmd(session, cmd, true);

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to execute command %s on the host %s", cmd, host), e);

            throw e;
        }
        return res;
    }

    /**
     * @param cmd Command to execute.
     * @return Command execution result.
     * @throws IOException If failed.
     * @throws InterruptedException If interrupted.
     */
    public CommandExecutionResult runLocCmd(String cmd) throws IOException {
        LOG.info(String.format("Running cmd %s", cmd));

        while (cmd.contains("  "))
            cmd = cmd.replace("  ", " ");

        String[] cmdArr = cmd.split(" ");

        final ProcessBuilder pb = new ProcessBuilder()
            .command(cmdArr);

        pb.directory(new File(args.getRemoteWorkDir()));

        Process proc = pb.start();

        int exitCode = 0;

        try {
            exitCode = proc.waitFor();
        }
        catch (InterruptedException e) {
            LOG.error(String.format("Failed to wait for command %s to complete", cmd), e);;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        String lineE;

        List<String> errStr = new ArrayList<>();

        BufferedReader errReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        while ((lineE = errReader.readLine()) != null) {
            LOG.error(String.format("Command '%s' returned error line: %s:", cmd, lineE));

            errStr.add(lineE);
        }

        String lineO;

        final List<String> outStr = new ArrayList<>();

        while ((lineO = reader.readLine()) != null)
            outStr.add(lineO);

        return new CommandExecutionResult(exitCode, outStr, errStr, proc);
    }

    public List<String> runStartCmd(String host, String cmd) throws Exception {
        List<String> res = new ArrayList<>();
        try {
            Session session = getSession(host);

            LOG.debug("Connected to the host: " + host);

            res = runRmtCmd(session, cmd, false);

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to execute command %s on the host %s", cmd, host));

            throw e;
        }

        return res;
    }

    /**
     * Check whether or not files exists on remote host.
     *
     * @param host {@code String} IP address of remote host.
     * @param paths {@code List} of {@code String} paths to files on remote host.
     * @return {@code true} if all files exists on remote host or {@code false} otherwise.
     * @throws Exception if failed.
     */
    public boolean checkRemoteFiles(String host, List<String> paths) throws Exception {
        for (String path : paths) {
            if (!exists(host, path)) {
                printer(String.format("Cannot execute operation because file %s is not found on the host %s", path, host));

                String depName = System.getProperty("os.name").contains("Windows") ? "deploy.bat" : "deploy.sh";

                println(String.format("Use %s to upload poc-tester files on remote hosts.", depName));

                return false;
            }
        }

        return true;
    }

    /**
     * Check whether or not file exists on remote host.
     *
     * @param host {@code String} IP address of remote host.
     * @param path {@code String} Path to file on remote host.
     * @return {@code true} if file exists on remote host or {@code false} otherwise.
     * @throws Exception if failed.
     */
    public boolean exists(String host, String path) throws Exception {
        try {
            Session ssn = getSession(host);

            LOG.debug("Connected to the host: " + host);

            Channel cnl = ssn.openChannel("exec");

            LOG.debug("Running command:");

            String checkCmd = "stat " + path;

            LOG.debug(checkCmd);

            ((ChannelExec)cnl).setCommand(checkCmd);

            cnl.setInputStream(null);
            ((ChannelExec)cnl).setErrStream(System.err);

            final BufferedReader errReader = new BufferedReader(new InputStreamReader(((ChannelExec)cnl).getErrStream()));

            cnl.connect(SSH_CHANNEL_TIMEOUT);

            boolean res = (errReader.readLine() == null);

            cnl.disconnect();

            ssn.disconnect();

            return res;
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to check file %s on the host %s", path, host), e);

            throw e;
        }
    }

    public void runListCmd(String host, List<String> cmds) throws Exception {
        List<String> res = new ArrayList<>();
        try {
            Session session = getSession(host);

            Channel channel = session.openChannel("exec");

            for (String cmd : cmds) {
                LOG.debug("Running command:");
                LOG.info(cmd);

                ((ChannelExec)channel).setCommand(cmd);

                channel.setInputStream(null);
                ((ChannelExec)channel).setErrStream(System.err);

                channel.connect(SSH_CHANNEL_TIMEOUT);

                channel.disconnect();
            }

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to execute commands on the host %s", host), e);

            throw e;
        }
    }

    private List<String> runRmtCmd(Session session, String cmd, boolean getResult) {
        List<String> res = new ArrayList<>();

        LOG.debug("Running command:");

        if(getResult)
            LOG.debug(cmd);
        else
            LOG.info(cmd);

        try {
            Channel channel = session.openChannel("exec");
            ((ChannelExec)channel).setCommand(cmd);

            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);

            BufferedReader reader = new BufferedReader(new InputStreamReader(channel.getInputStream()));

            final BufferedReader errReader = new BufferedReader(new InputStreamReader(((ChannelExec)channel).getErrStream()));

            channel.connect(SSH_CHANNEL_TIMEOUT);

            Thread errThread = new Thread() {
                @Override public void run() {

                    String errLine;

                    try {
                        while ((errLine = errReader.readLine()) != null) {
                            if (!excluded(errLine))
                                printer(errLine);

                            LOG.error(errLine);
                        }
                    }
                    catch (IOException e) {
                        LOG.error("Failed to read error stream.", e);
                    }
                }
            };

            errThread.setDaemon(true);

            errThread.start();

            String nextLine;

            if (getResult) {
                while ((nextLine = reader.readLine()) != null)
                    res.add(nextLine);
            }
            else {
                int cntr = 0;
                while ((nextLine = reader.readLine()) != null && cntr++ < 4150) {
                    LOG.info(nextLine);

                    if (included(nextLine))
                        res.add(nextLine);

                    if (nextLine.contains("Topology snapshot") || nextLine.contains("Failed to") ||
                        nextLine.contains("Task started without launching ignite node.")) {

                        if(nextLine.contains("Failed to connect to any address from IP finder"))
                            res.add("No servers were found. Please  use bin/start-servers.sh to start at least one data node.");
                        else
                            res.add(nextLine);

                        break;
                    }
                }
            }

            channel.disconnect();

            int exitStatus = channel.getExitStatus();

            LOG.debug("Done with exit status: " + exitStatus);

            if (exitStatus != 0) {
                if (!res.isEmpty()) {
                    LOG.info("Command output:");

                    for (String str : res)
                        LOG.info(str);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return res;
    }

    public void put(String host, String src, String dst) throws Exception {
        if (new File(src).isDirectory()) {
            copyDir(host, src, dst);
            return;
        }

        try {
            Session session = getSession(host);

            ChannelSftp sftpChannel = (ChannelSftp)session.openChannel("sftp");
            sftpChannel.connect(SSH_CHANNEL_TIMEOUT);

            sftpChannel.put(src, dst);

            sftpChannel.disconnect();

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to copy file %s on the host %s", src, host), e);

            throw e;
        }
    }

    public void get(String host, String src, String dst) throws Exception {
        try {
            Session session = getSession(host);

            ChannelSftp sftpChannel = (ChannelSftp)session.openChannel("sftp");
            sftpChannel.connect(SSH_CHANNEL_TIMEOUT);

            sftpChannel.get(src, dst);

            sftpChannel.disconnect();

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to get file %s from the host %s. Error message: %s", src, host,
                e.getMessage()));

            throw e;
        }
    }

    public void compressFile(String host, String filePath) throws Exception {
        File inFile = new File(filePath);

        String inFileName = inFile.getName();
        String inParentDir = inFile.getParent();
        String outFileName = inFileName + ".zip";

        String cmd = String.format("cd %s; zip -r %s %s",
                inParentDir, outFileName, inFileName);

        LOG.info("Compressing {} on host {}", filePath, host);

        try {
            Session session = getSession(host);

            runRmtCmd(session, cmd, false);

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error("Failed to compress {} on host {}", filePath, host);
            LOG.error(e);
            throw e;
        }
    }

    public void removeFile(String host, String filePath) throws Exception {
        File inFile = new File(filePath);

        // TODO: replace all special characters
        filePath.replace(" ", "\\ ");

        String cmd = String.format("rm -rf \"%s\"", filePath);

        LOG.info("Removing {} on host {}", filePath, host);

        try {
            Session session = getSession(host);

            runRmtCmd(session, cmd, false);

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error("Failed to remove {} on host {}", filePath, host);
            LOG.error(e);
            throw e;
        }
    }

//    private void uploadDir(String host, String src, String dst) {
//        println(String.format("Uploading %s directory to the host %s", src, host));
//
//        String tempZipName = "temp-" + dateTime() + ".zip";
//
//        File srcDir = new File(src);
//
//        List<String> fileList = new ArrayList<>();
//
//        List<String> folderList = new ArrayList<>();
//
//        File[] fileArr = srcDir.listFiles();
//
//        if (fileArr == null) {
//            printer(String.format("Failed to upload dir %s on the host %s", src, host));
//
//            return;
//        }
//
//        for (File file : fileArr) {
//            if (file.isFile())
//                fileList.add(file.getName());
//
//            if (file.isDirectory())
//                folderList.add(file.getName());
//        }
//
//        String srcZipPath = Paths.get(locHome, tempZipName).toString();
//
//        try {
//            PocTesterUtils.createZip(src, srcZipPath, fileList, folderList);
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        String rmDstCmd = "rm -rf " + dst;
//
//        runCmd(host, rmDstCmd);
//
//        String mkdirCmd = "mkdir -p " + dst;
//
//        runCmd(host, mkdirCmd);
//
//        String dstZipPath = String.format("%s/%s", dst, tempZipName);
//
//        put(host, srcZipPath, dstZipPath);
//
//        String unzipCmd = String.format("unzip -q %s -d %s", dstZipPath, dst);
//
//        runCmd(host, unzipCmd);
//
//        String delCmd = "rm -f " + dstZipPath;
//
//        runCmd(host, delCmd);
//
//        new File(srcZipPath).delete();
//
//        println("Done.");
//    }

    public void copyDir(String host, String srcDir, String dstDir) throws Exception {
//        println(String.format("Uploading %s directory to the host %s", srcDir, host));

        try {
            Session session = getSession(host);

            runRmtCmd(session, "mkdir -p " + dstDir, true);

            ChannelSftp sftpChannel = (ChannelSftp)session.openChannel("sftp");
            sftpChannel.connect(SSH_CHANNEL_TIMEOUT);

            File[] fileArr = new File(srcDir).listFiles();

            if (fileArr == null) {
                printer("Failed to copy directory " + srcDir);

                return;
            }

            for (File file : fileArr) {
                if (file.isDirectory()) {
                    String dirPath = file.getAbsolutePath();

                    String dirName = file.getName();

                    String dstPath = String.format("%s/%s", dstDir, dirName);

                    copyDir(host, dirPath, dstPath);
                    continue;
                }

                String fileName = file.getName();

                String src = file.getAbsolutePath();

                String dst = String.format("%s/%s", dstDir, fileName);

                sftpChannel.put(src, dst);
            }
            sftpChannel.disconnect();

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to copy directory %s on the host %s", srcDir, host), e);

            throw e;
        }

//        println("Done.");
    }

    /**
     *
     * @param host {@code String} host IP.
     * @param type {@code NodeType} node type.
     * @return {@code Map} where key is process id and value is node consistent ID.
     * @throws Exception if failed.
     */
    public Map<String, String> getPidMap(String host, @Nullable NodeType type) throws Exception{
        Map<String, String> res = new HashMap<>();

        String prefix = "-DCONSISTENT_ID=poc-tester";
        String filter = "java";
        if (type != null)
            if (type == NodeType.ZOOKEEPER) {
                prefix = "-DCONSISTENT_ID=";
                filter = "org.apache.zookeeper.server";
            }
            else {
                // Either POC Tester's server node or client node
                prefix = "-DCONSISTENT_ID=poc-tester-" + type;
                filter = "org.apache.ignite.scenario";
            }

        try {
            Session ses = getSession(host);

            List<String> pidList = runRmtCmd(
                    ses,
                    String.format("pgrep -a 'java' | grep %s | awk '{print $1}'", filter),
                    true);

            for (String pid : pidList) {
                List<String> argList = runCmd(host, "ps -p " + pid + " -o comm,args=ARGS");

                for (String arg : argList) {

                    String[] argArr = arg.split(" ");

                    for (String str : argArr) {
                        if (str.startsWith(prefix))
                            res.put(pid, str.replace("-DCONSISTENT_ID=", ""));

                    }
                }
            }

            ses.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to get pid map from the host %s", host), e);

            throw e;
        }

        return res;
    }

    private boolean excluded(String line) {
        return line.startsWith("log4j:WARN No appenders could be found for logger (org.springframework.core.env.StandardEnvironment)")
            || line.startsWith("log4j:WARN Please initialize the log4j system properly.")
            || line.contains("New version is available at ignite.apache.org:")
            || line.startsWith("log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.");
    }

    private boolean included(String line) {
        return (line.contains("Using connection string: ")
            || line.contains("Will insert organisations")
            || line.contains("will not insert any data")
            || line.contains("is starting load. Will load keys from")
            || line.contains("DEBUG_MSG"));
    }
}


