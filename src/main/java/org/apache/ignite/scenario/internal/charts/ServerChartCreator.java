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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;

/**
 * Created by oostanin on 13.12.17.
 */
public class ServerChartCreator {

    private static final String[] useList = new String[] {"Found stop flag", "Starting to count", "Current sum", "Topology snapshot"};

    private File mainDir;

    private List<File> fileList;

    private Map<Long, String> checkpointMap;
    private Map<String, Set<Long>> cachePartMap;

    private Set<String> logSet;

    private long firstTimeStamp;
    private long lastTimeStamp;

    public ServerChartCreator(File mainDir) {
        this.mainDir = mainDir;

        cachePartMap = new HashMap<>();

        logSet = new TreeSet<>();
    }

    public static void main(String[] args) {
        ServerChartCreator creator = new ServerChartCreator(new File("/home/oostanin/gg-qa/gg-qa/poc-tester/target/assembly/log-2018-03-01-08-08-17/clients"));

        creator.setCheckpointMap();
    }

    public XYChart getServerChart(List<PocTaskReport> reports) {
        checkpointMap = new HashMap<>();

        setCheckpointMap();

        for (String s : cachePartMap.keySet()) {
            System.out.println(s);

            ;
        }

        return mapToChart();
    }

    private void setTimeStamps(List<PocTaskReport> reports) {
        firstTimeStamp = Long.MAX_VALUE;
        lastTimeStamp = 0L;

        for (PocTaskReport report : reports) {
            if (report.getFirstTimeStamp() < firstTimeStamp)
                firstTimeStamp = report.getFirstTimeStamp();

            if (report.getLastTimeStamp() > lastTimeStamp)
                lastTimeStamp = report.getLastTimeStamp();
        }
    }

    private XYChart mapToChart() {
        List<Long> timePntsList = new ArrayList<>();
        List<Date> dateList = new ArrayList<>();
        List<Long> valList = new ArrayList<>();

        for (Map.Entry<Long, String> entry : checkpointMap.entrySet()) {
            Long timePnt = entry.getKey();
            timePntsList.add(timePnt);
        }
        Collections.sort(timePntsList);

        long lastStartPoint = 0;

        for (int i = 0; i < timePntsList.size(); i++) {
            long ts = timePntsList.get(i);

            if (ts < firstTimeStamp && ts > lastTimeStamp)
                continue;

            String event = checkpointMap.get(ts);

            if (event.equals("started")) {
                for (int j = (i + 1); j < timePntsList.size(); j++) {
                    if (checkpointMap.get(timePntsList.get(j)).equals("finished")) {
                        lastStartPoint = ts;
                        dateList.add(new Date(ts));
                        valList.add(1L);
                        long finishPoint = timePntsList.get(j);
                        long chekpointDuration = finishPoint - lastStartPoint;
                        dateList.add(new Date(ts + 1));
                        valList.add(chekpointDuration);
                        dateList.add(new Date(ts + 2));
                        valList.add(1L);
//                        dateList.add(new Date(finishPoint));
//                        valList.add(1L);
                        break;
                    }
                }
            }
        }

        XYChart checpointChart = new XYChartBuilder().width(1200).height(600).title("checkpoints").build();

        checpointChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        checpointChart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
        //checpointChart.getStyler().setYAxisLogarithmic(true);

        if (dateList.size() == 0 || valList.size() == 0)
            return null;

        XYSeries seriesAvg = checpointChart.addSeries("checkpoints", dateList, valList);
        seriesAvg.setMarker(SeriesMarkers.NONE);
        seriesAvg.setLineWidth(1.0F);

        return checpointChart;
    }

    private void setCheckpointMap() {
        fileList = getServerLogList();

        for (File logFile : fileList) {
            try {
                parse(logFile);
            }
            catch (IOException e) {
                System.out.println("File: " + logFile.getAbsolutePath());
                e.printStackTrace();
            }
        }

        List<String> cacheList = new ArrayList<>();

        for (String s : cachePartMap.keySet())
            cacheList.add(s);

        Collections.sort(cacheList);

        for (String cache : cacheList) {
            Set<Long> parts = cachePartMap.get(cache);

            List<Long> partList = new ArrayList<>();

            for (Long part : parts)
                partList.add(part);

            Collections.sort(partList);

            System.out.print(String.format("%s -> %d    [ ", cache, cachePartMap.get(cache).size()));

            for (Long part : partList)
                System.out.print(String.format(" %d ", part));

            System.out.println(" ]");
        }

        List<String> logList = new ArrayList<>();

        for(String log : logSet)
            logList.add(log);

        Collections.sort(logList);

        new File("/home/oostanin/gg-qa/gg-qa/poc-tester/target/assembly/test.log").delete();

        try {
            PocTesterUtils.updateFile(
                Paths.get("/home/oostanin/gg-qa/gg-qa/poc-tester/target/assembly/test.log"), logList);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void parse(File file) throws IOException {
        if (file.getName().startsWith("gc-"))
            return;

        if (file.getName().endsWith(".gz")) {
            File tempDecompressedFile = new File(file.getAbsolutePath().replace(".gz", ".temp"));
            unGunzipFile(file.getAbsolutePath(), tempDecompressedFile.getAbsolutePath());
            setMap(tempDecompressedFile);
            tempDecompressedFile.delete();
        }
        setMap(file);
    }

    private void setMap(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line;

        while ((line = reader.readLine()) != null) {
            if (!line.startsWith("[2"))
                continue;

//            Long ts = null;
//
//            try {
//                ts = getTimeStamp(line);
//            }
//            catch (ParseException e) {
//                e.printStackTrace();
//                System.out.println("Error in file: " + file.getAbsolutePath());
//                System.out.println("Error in line: " + line);
//            }
//
//            if (line.contains("Checkpoint started") && ts != null)
//                checkpointMap.put(ts, "started");
//
//            if (line.contains("Checkpoint finished") && ts != null)
//                checkpointMap.put(ts, "finished");

            if (line.contains("Failed to find value for key")) {
                int dotInd = line.indexOf('.');
                int start = line.lastIndexOf("cachepoc");

                int failStart = line.lastIndexOf("Failed to find");

                int inStart = line.lastIndexOf(" in cache ");

                String cacheName = line.substring(start, dotInd);

                String keyStr = line.substring(failStart + 29, inStart);

                long key = Long.valueOf(keyStr);

                long part = key % 1024;

                if (!cachePartMap.keySet().contains(cacheName)) {
                    Set<Long> partSet = new TreeSet<>();

                    partSet.add(part);

                    cachePartMap.put(cacheName, partSet);
                }
                else
                    cachePartMap.get(cacheName).add(part);

//                System.out.println(keyStr);
//
//                System.out.println(cacheName);
            }

            if (line.contains("Setting") && line.contains("flag to"))
                logSet.add(line);

            for (String useStr : useList) {
                if (line.contains(useStr))
                    logSet.add(line);
            }

        }
    }

//    private List<File> getServerLogList() {
//        List<File> res = new ArrayList<>();
//
//        final File[] hostDirs = PocTesterUtils.getSubDirs(mainDir.toPath(), null);
//
//        for (File hostDir : hostDirs) {
//
//            File[] serverLogs = hostDir.listFiles();
//
//            for (File serverLog : serverLogs)
//                res.add(serverLog);
//        }
//
//        return res;
//    }

    private List<File> getServerLogList() {
        List<File> res = new ArrayList<>();

        final File[] hostDirs = PocTesterUtils.getSubDirs(mainDir.toPath(), null);

        for (File hostDir : hostDirs) {

            File[] serverDirs = PocTesterUtils.getSubDirs(hostDir.toPath(), "task-");

            for (File serverDir : serverDirs) {
                File[] serverLogs = serverDir.listFiles();

                for (File serverLog : serverLogs) {
                    if (serverLog.getName().endsWith(".log") || serverLog.getName().endsWith(".log.gz"))
                        res.add(serverLog);
                }

            }
        }

        return res;
    }

    private Long getTimeStamp(String line) throws ParseException {
        String dateTime = line.substring(1, 20);

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");

        return format.parse(dateTime).getTime();
    }

    public void unGunzipFile(String compressedFile, String decompressedFile) {

        byte[] buffer = new byte[1024];

        try {

            FileInputStream fileIn = new FileInputStream(compressedFile);

            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);

            FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);

            int bytes_read;

            while ((bytes_read = gZIPInputStream.read(buffer)) > 0)
                fileOutputStream.write(buffer, 0, bytes_read);

            gZIPInputStream.close();
            fileOutputStream.close();

        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
