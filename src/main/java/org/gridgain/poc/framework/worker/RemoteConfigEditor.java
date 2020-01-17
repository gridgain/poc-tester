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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.worker.PrepareWorker.CFG_BACKUPS_PLACEHOLDER;
import static org.gridgain.poc.framework.worker.PrepareWorker.CFG_PART_LOSS_POLICY_PLACEHOLDER;

/**
 * Created by oostanin on 21.12.17.
 */
public class RemoteConfigEditor extends AbstractWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(RemoteConfigEditor.class.getName());

    private List<String> backupsList = new ArrayList<>();

    public RemoteConfigEditor(PocTesterArguments args) {
        this.args = args;
    }

    public static void main(String[] args) {

        PocTesterArguments args0 = new PocTesterArguments();
        args0.setArgsFromCmdLine(args);
        args0.setArgsFromStartProps();

//        try {
//            new RemoteConfigEditor().edit(args0);
//        }
//        catch (IOException e) {
//            LOG.error("Failed to edit config files.");
//            e.printStackTrace();
//        }
    }

    public void insertIpAddresses(String pathOrig, String pathRes, List<String> ipList, String portRange)
        throws IOException {
        List<String> resLines = new ArrayList<>();

        checkFile(pathOrig);

        try (BufferedReader br = new BufferedReader(new FileReader(pathOrig))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("<!--to_be_replaced_by_IP_list-->"))
                    addIpToList(resLines, ipList, portRange);
                else
                    resLines.add(line);
            }
        }

        PocTesterUtils.saveFile(Paths.get(pathRes), resLines);
    }

    public void editConfig(String pathOrig, String pathRes, String baseCfgName, List<String> ipList,
        String portRange) throws IOException {
        List<String> resLines = new ArrayList<>();

        checkFile(pathOrig);

        try (BufferedReader br = new BufferedReader(new FileReader(pathOrig))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("<!--to_be_replaced_by_IP_list-->"))
                    addIpToList(resLines, ipList, portRange);
                else if (line.contains("<import resource=\"ignite-base-config.xml\"/>"))
                    resLines.add(String.format("    <import resource=\"prepared-%s\"/>", baseCfgName));
                else
                    resLines.add(line);
            }
        }

        PocTesterUtils.saveFile(Paths.get(pathRes), resLines);
    }

    public void editBaseConfig(String pathOrig, String pathRes, PocTesterArguments args) throws IOException {
        List<String> resLines = new ArrayList<>();

        checkFile(pathOrig);

        try (BufferedReader br = new BufferedReader(new FileReader(pathOrig))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.matches(".*" + CFG_BACKUPS_PLACEHOLDER + ".*"))
                    line = line.replaceAll(CFG_BACKUPS_PLACEHOLDER, args.getBackupsNext());

                if (line.matches(".*" + CFG_PART_LOSS_POLICY_PLACEHOLDER + ".*"))
                    line = line.replaceAll(CFG_PART_LOSS_POLICY_PLACEHOLDER, args.getPartitionLossPolicy());

                resLines.add(line);
            }
        }

        PocTesterUtils.saveFile(Paths.get(pathRes), resLines);
    }

    public void editZkCfg(String pathOrig, String pathRes, String baseCfgName, List<String> ipList, String portRange)
        throws IOException {
        List<String> resLines = new ArrayList<>();

        checkFile(pathOrig);

        String zkConnStr = getZkConnStr();

        try (BufferedReader br = new BufferedReader(new FileReader(pathOrig))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("to_be_replaced_with_actual_value"))
                    resLines.add(line.replace("to_be_replaced_with_actual_value", zkConnStr));
                else if (line.contains("<import resource=\"ignite-base-config.xml\"/>"))
                    resLines.add(String.format("    <import resource=\"prepared-%s\"/>", baseCfgName));
                else
                    resLines.add(line);
            }
        }

        PocTesterUtils.saveFile(Paths.get(pathRes), resLines);
    }

    public void prepareZkCfg() throws IOException {
        String zkCfgTempl = String.format("%s/config/cluster/zookeeper/template-zoo.cfg", locHome);
        String javaEnvTempl = String.format("%s/config/cluster/zookeeper/template-java.env", locHome);

        String preparedZkCfg = String.format("%s/config/cluster/zookeeper/prepared-zoo.cfg", locHome);
        String preparedJavaEnv = String.format("%s/config/cluster/zookeeper/prepared-java.env", locHome);

        String zkDataDir = String.format("%s/zookeeper/data", args.getRemoteWorkDir());
        String zkLogDir = String.format("%s/log/zookeeper", args.getRemoteWorkDir());

        List<String> zkCfgResLines = new ArrayList<>();

        List<String> zkHostList = PocTesterUtils.getHostList(args.getZooKeeperHosts(), true);

        List<String> toCfg = new ArrayList<>(zkHostList.size());

        for (int i = 0; i < zkHostList.size(); i++)
            toCfg.add(String.format("server.%d=%s:2888:3888", i + 1, zkHostList.get(i)));

        try (BufferedReader br = new BufferedReader(new FileReader(zkCfgTempl))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("to_be_replaced_with_actual_list"))
                    zkCfgResLines.addAll(toCfg);
                else if (line.contains("path_to_data_dir"))
                    zkCfgResLines.add(line.replace("path_to_data_dir", zkDataDir));
                else
                    zkCfgResLines.add(line);
            }
        }

        PocTesterUtils.saveFile(Paths.get(preparedZkCfg), zkCfgResLines);

        List<String> javaEnvRes = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(javaEnvTempl))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("path_to_logs_dir"))
                    javaEnvRes.add(line.replace("path_to_logs_dir", zkLogDir));
                else
                    javaEnvRes.add(line);
            }
        }

        PocTesterUtils.saveFile(Paths.get(preparedJavaEnv), javaEnvRes);
    }

    private String getZkConnStr() {
        StringBuilder sb = new StringBuilder();

        for (String servHost : PocTesterUtils.getHostList(args.getZooKeeperHosts(), true)) {
            if (!sb.toString().isEmpty())
                sb.append(",");

            sb.append(String.format("%s:2181", servHost));
        }

        return sb.toString();
    }

    private void addIpToList(List<String> resLines, List<String> ipList, String portRange) {
        for (String ip : ipList) {
            String toAdd = "        " + "<value>" + ip + ":" + portRange + "</value>";
            resLines.add(toAdd);
        }
    }

    private boolean checkFile(String pathOrig) {
        if (!new File(pathOrig).exists()) {
            LOG.error(String.format("Failed to find file %s", pathOrig));

            System.exit(1);
        }

        return true;
    }

    protected void printHelp() {
    }


}
