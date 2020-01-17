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

package org.gridgain.poc.framework.ssh;

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

import org.gridgain.poc.framework.worker.NodeType;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;

public class LocalCmdExecutor extends AbstractCmdExecutor {
    private static final Logger LOG = LogManager.getLogger(LocalCmdExecutor.class.getName());

    /**
     *
     * @param args
     */
    public LocalCmdExecutor(PocTesterArguments args){
        super(args);
    };

    public List<String> runCmd(String host, String cmd) throws Exception {
        List<String> res = new ArrayList<>();

        Process p;

        LOG.info(String.format("Executing command %s", cmd));

        try {
            p = Runtime.getRuntime().exec(cmd);

//            p.waitFor();

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line;

            while ((line = reader.readLine())!= null) {
//                LOG.info("Line = " + line);

                res.add(line);
            }

        } catch (Exception e) {
            LOG.error(String.format("Failed to execute command %s on the host %s", cmd, host));

            throw e;
        }

        return res;
    }

//    public List<String> runStartCmd(String host, String cmd) throws Exception {
//        List<String> res = new ArrayList<>();
//        try {
//            Session session = getSession(host);
//
//            LOG.info("Connected to the host: " + host);
//
//            res = runRmtCmd(session, cmd, false);
//
//            session.disconnect();
//        }
//        catch (Exception e) {
//            LOG.error(String.format("Failed to execute command %s on the host %s", cmd, host));
//
//            throw e;
//        }
//        return res;
//    }
//
//    public boolean checkRemoteFiles(String host, List<String> paths) throws Exception {
//        for(String path : paths){
//            if(!exists(host, path)){
//                printer(String.format("Cannot execute operation because file %s is not found on the host %s", path, host));
//
//                String depName = System.getProperty("os.name").contains("Windows") ? "deploy.bat" : "deploy.sh";
//
//                println(String.format("Use %s to upload poc-tester files on remote hosts.", depName));
//
//                return false;
//            }
//        }
//
//        return true;
//    }

    public boolean exist(String host, String path) throws Exception {
        boolean res = false;

        String checkCmd = "stat " + path;

        try {
            Session session = getSession(host);

            LOG.info("Connected to the host: " + host);

            Channel channel = session.openChannel("exec");

            LOG.info("Running command:");
            LOG.info(checkCmd);

            ((ChannelExec)channel).setCommand(checkCmd);

            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);

            final BufferedReader errReader = new BufferedReader(new InputStreamReader(((ChannelExec)channel).getErrStream()));

            channel.connect(60_000);

            if(errReader.readLine() != null)
                res = false;
            else
                res = true;

            channel.disconnect();

            session.disconnect();
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to check file %s on the host %s", path, host), e);

            throw e;
        }

        return res;
    }

//    public void runListCmd(String host, List<String> cmds) {
//        List<String> res = new ArrayList<>();
//        try {
//            Session session = getSession(host);
//
//            Channel channel = session.openChannel("exec");
//
//            for (String cmd : cmds) {
//                LOG.info("Running command:");
//                LOG.info(cmd);
//
//                ((ChannelExec)channel).setCommand(cmd);
//
//                channel.setInputStream(null);
//                ((ChannelExec)channel).setErrStream(System.err);
//
//                channel.connect(60_000);
//
//                channel.disconnect();
//            }
//
//            session.disconnect();
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    private List<String> runRmtCmd(Session session, String cmd, boolean getResult) {
        List<String> res = new ArrayList<>();

        LOG.info("Running command:");
        LOG.info(cmd);

        try {
            Channel channel = session.openChannel("exec");
            ((ChannelExec)channel).setCommand(cmd);

            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);

            BufferedReader reader = new BufferedReader(new InputStreamReader(channel.getInputStream()));

            final BufferedReader errReader = new BufferedReader(new InputStreamReader(((ChannelExec)channel).getErrStream()));

            channel.connect(60_000);

            Thread errThread = new Thread() {
                @Override public void run() {

                    String errLine;

                    try {
                        while ((errLine = errReader.readLine()) != null)
                            if(!excluded(errLine))
                                printer(errLine);
                    }
                    catch (IOException e) {
                        e.printStackTrace();
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
                while ((nextLine = reader.readLine()) != null && cntr++ < 150) {
                    LOG.info(nextLine);

                    if (nextLine.contains("Topology snapshot") || nextLine.contains("Failed to") ||
                        nextLine.contains("Task started without launching ignite node.")) {

                        res.add(nextLine);

                        break;
                    }
                }
            }

            channel.disconnect();

            int exitStatus = channel.getExitStatus();

            LOG.info("Done with exit status: " + exitStatus);

            if (exitStatus != 0) {
                if (res.size() > 0) {
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
            sftpChannel.connect(60_000);

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
            sftpChannel.connect(60_000);

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
            sftpChannel.connect(60_000);

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

    public Map<String, String> getPidMap(String host, NodeType type) throws Exception {
        Map<String, String> res = new HashMap<>();

        String prefix = type == null ? "-DCONSISTENT_ID=poc-tester" : "-DCONSISTENT_ID=poc-tester-" + type;

        try {
            Session ses = getSession(host);

            List<String> pidList = runRmtCmd(ses, "pgrep 'java'", true);

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

    private boolean excluded(String line){
        return line.startsWith("log4j:WARN No appenders could be found for logger (org.springframework.core.env.StandardEnvironment)") ||
            line.startsWith("log4j:WARN Please initialize the log4j system properly.") ||
            line.startsWith("log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.");
    }
}


