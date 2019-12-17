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

package org.apache.ignite.scenario.internal.charts;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.knowm.xchart.XYChart;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.getSubDirs;

/**
 * Class for assembling data from different hosts.
 */
public class DataAssembler {
    /** */
    private static final Logger LOG = LogManager.getLogger(DataAssembler.class.getName());

    /** Main directory with result files from hosts. */
    private File mainDir;

    /** Map containing task names and directories. Key - task name, value - collection of directories from one or more
     * hosts with task result data. */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<File>> taskMap;

    /**
     * @param mainDir Main directory with result files from hosts.
     */
    DataAssembler(File mainDir) {
        this.mainDir = mainDir;
    }

    /** */
    public List<XYChart>  getChartList() {
        // Fill the task map.
        taskMap = assembleTaskMap();

        // List of reports for creating JSON files.
        List<PocTaskReport> taskReports = new ArrayList<>();

        for (Map.Entry<String, ConcurrentLinkedQueue<File>> entry : taskMap.entrySet()) {
            String str = entry.getKey();

            try {
                PocTaskReport report = new ReportCreator(str, taskMap.get(str)).createReport();

                if (report != null)
                    taskReports.add(report);

            }
            catch (Exception e) {
                LOG.error(String.format("Failed to create report for the task %s", str), e);
            }
        }

        for (PocTaskReport report : taskReports) {
            Map<Object, Object> toJson = new HashMap<>();

            toJson.put("name", report.getTaskName());
            toJson.put("startTime", report.getFirstTimeStamp());
            toJson.put("finishTime", report.getLastTimeStamp());

            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            String dateFormatted = formatter.format(new Date(report.getFirstTimeStamp()));

            String id = report.getTaskName() + "-" + dateFormatted;
            report.setTaskID(id);
            toJson.put("id", id);

            Map<Object, Object> propMap = report.getPropMap();

            Map<Object, Object> hdrMap = new HashMap<>();
            hdrMap.put("time", "milliseconds");

            String data = "unknown";
            String unit = "unknown";

            hdrMap.put(data, unit);

            boolean printOps = false;

            if (propMap != null) {
                toJson.put("taskProperties", report.getPropMap());

                if (propMap.get("unit") != null)
                    unit = propMap.get("unit").toString();

                if (propMap.get("data") != null)
                    data = propMap.get("data").toString();

                hdrMap.put(data, unit);

                if (propMap.get("headersMap") != null) {
                    hdrMap = (Map<Object, Object>)propMap.get("headersMap");
                    data = hdrMap.get("data") == null ? "unknown" : hdrMap.get("data").toString();
                }

                printOps = propMap.get("reportDir") != null && propMap.get("reportDir").equals("eventstore");
            }
            else
                LOG.warn("Failed to load task property map from task result directory.");

            toJson.put("headers", hdrMap);

            List<Date> timePntList = report.getDateList();
            List<Long> mainList = report.getMainList();
            List<Long> opsList = report.getOperCntList();

            List<Map<String, Long>> dotList = new ArrayList<>(timePntList.size());

            for (int cntr = 0; cntr < timePntList.size(); cntr++) {
                Map<String, Long> pntMap = new HashMap<>();
                pntMap.put("time", timePntList.get(cntr).getTime());

                pntMap.put(data, mainList.get(cntr));

                if (printOps)
                    pntMap.put("ops", opsList.get(cntr));

                dotList.add(pntMap);
            }

            toJson.put("data", dotList);

            Path jsonPath = Paths.get(mainDir.getAbsolutePath(), "report", report.getReportDirName(),
                report.getTaskName() + ".json");

            try {
                PocTesterUtils.saveReport(jsonPath, toJson);
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }

        return new PocChartBuilder().buildCharts(taskReports);
    }

    /**
     * Concurrently fill task map with data.
     * @return {@code ConcurrentHashMap} task map.
     */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<File>> assembleTaskMap() {
        // Getting array of host directories.
        Path clientsDir = Paths.get(mainDir.getAbsolutePath(), "clients");

        if(!clientsDir.toFile().exists()){
            LOG.error(String.format("Failed to find 'clients' directory in the result directory %s", mainDir));

            return null;
        }

        final File[] hostDirs = getSubDirs(Paths.get(mainDir.getAbsolutePath(), "clients"), null);

        if(hostDirs == null || hostDirs.length == 0){
            LOG.error(String.format("Failed to find client host directories in 'clients' directory %s", clientsDir));

            return null;
        }

        taskMap = new ConcurrentHashMap<>();

        ExecutorService execServ = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        Collection<Future<?>> futList = new ArrayList<>();

        for (File hostDir : hostDirs) {
            Path hostDirPath = hostDir.toPath();

            futList.add(execServ.submit(new FileWorker(hostDirPath)));
        }

        for (Future f : futList) {
            try {
                f.get();
            }
            catch (Exception e) {
                LOG.error("Failed to assemble task map.", e);

                e.printStackTrace();
            }
        }

        execServ.shutdown();

        String endStr = taskMap.size() == 1 ? " task" : " tasks";

        System.out.println();
        System.out.println("Found " + taskMap.size() + endStr + " in result directory.");
        System.out.println();

        return taskMap;
    }

    /**
     * Fills task map with data from particular host directory.
     */
    private class FileWorker implements Runnable {
        /** */
        private final Path hostDir;

        /**
         * Constructor.
         *
         * @param hostDir {@code Path} Host directory.
         */
        FileWorker(Path hostDir) {
            this.hostDir = hostDir;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if(!hostDir.toFile().exists()){
                LOG.error(String.format("Failed to find host directory %s", hostDir));

                return;
            }

            // Getting array of task directories from host directory.
            File[] taskDirs = getSubDirs(hostDir, "task-");

            if(taskDirs == null || taskDirs.length == 0){
                LOG.error(String.format("Failed to find task directories in the host directory %s", hostDir));

                return;
            }

            for (File taskDir : taskDirs) {
                String taskDirName = taskDir.getName();

                File taskReportDir = Paths.get(mainDir.getAbsolutePath(), "report", taskDirName).toFile();

                if (!taskReportDir.exists())
                    taskReportDir.mkdir();

                System.out.println("Processing directory " + taskDir.getName());

                File[] evtFiles = getSubDirs(hostDir, taskDirName);

                ConcurrentLinkedQueue<File> resFiles = taskMap.get(taskDirName);

                if (resFiles == null) {
                    ConcurrentLinkedQueue<File> newFiles = new ConcurrentLinkedQueue<>();

                    resFiles = taskMap.putIfAbsent(taskDirName, newFiles);

                    if (resFiles == null)
                        resFiles = newFiles;
                }

                resFiles.addAll(Arrays.asList(evtFiles));
            }
        }
    }
}
