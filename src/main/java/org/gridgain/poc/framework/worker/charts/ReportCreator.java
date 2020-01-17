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

package org.gridgain.poc.framework.worker.charts;

import au.com.bytecode.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;

/**
 * Created by oostanin on 04.12.17.
 */
public class ReportCreator {
    /** */
    private static final Logger LOG = LogManager.getLogger(ReportCreator.class.getName());

    /** */
    private long firstTimeStamp;

    /** */
    private long lastTimeStamp;

    /** */
    private int mainWidth;

    /** */
    private long interval;

    /** */
    private String dirName;

    /** List of task result files. */
    private ConcurrentLinkedQueue<File> files;

    /** List of directories containing task result data. */
    private ConcurrentLinkedQueue<File> taskDirs;

    /** */
    //private ConcurrentHashMap<Long, ConcurrentLinkedQueue<Long>> dotMap;

    /** */
    private ConcurrentHashMap<Long, ConcurrentLinkedQueue<ReportInterval>> intervalMap;

    /** */
    public ReportCreator(String dirName, ConcurrentLinkedQueue<File> taskDirs) {
        this.dirName = dirName;
        this.taskDirs = taskDirs;
        this.firstTimeStamp = 0;
    }

    /** */
    public PocTaskReport createReport() throws Exception {
        if (taskDirs == null || taskDirs.isEmpty())
            return null;

        //dotMap = new ConcurrentHashMap<>();
        intervalMap = new ConcurrentHashMap<>();


        files = new ConcurrentLinkedQueue<>();

        // All 'task-properties.json' files for the task should be the same so we peek one such file end use it for
        // property determination.
        Path propPath = Paths.get(taskDirs.peek().getAbsolutePath(), "task-properties.json");

        String reportDirName = null;

        try {
            Map<Object, Object> propMap = PocTesterUtils.readMap(propPath);

            if (propMap != null) {
                reportDirName = propMap.get("reportDir").toString();

                for (File taskDir : taskDirs) {
                    File dataDir = new File(taskDir, reportDirName);

                    File[] fileArr = getEventFiles(dataDir);

                    if(fileArr != null)
                        files.addAll(Arrays.asList(fileArr));
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }

        if (files.isEmpty())
            return null;

        setBorderTimeStamps();

        long duration = lastTimeStamp - firstTimeStamp;

        System.out.println("Creating report for " + dirName + " directory");

        mainWidth = (int)(duration / 10000);

        if (mainWidth < 1200)
            mainWidth = 1200;

        interval = (lastTimeStamp - firstTimeStamp) / mainWidth;

        setMap(reportDirName);

        PocTaskReport report = mapToReport();

        if (report == null)
            return null;

        try {

            Map<Object, Object> propMap;

            propMap = PocTesterUtils.readMap(propPath);

            if (propMap != null) {
                report.setTaskName(propMap.get("MAIN_CLASS").toString());
                report.setPropMap(propMap);
            }
            else
                report.setTaskName(dirName);

        }
        catch (Exception e) {
            LOG.error("Failed to read property map rom file " + propPath);
            e.printStackTrace();
        }

        report.setFirstTimeStamp(firstTimeStamp);
        report.setLastTimeStamp(lastTimeStamp);
        report.setReportDirName(dirName);

        return report;
    }

    private void setBorderTimeStamps() throws Exception {
        firstTimeStamp = Long.MAX_VALUE;
        lastTimeStamp = 0L;

        for (File file : files) {

            CSVReader reader = new CSVReader(new BufferedReader(new FileReader(file.getAbsolutePath())));
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length > 0 && nextLine[0].contains("Timestamp"))
                    continue;

                if (nextLine.length > 0) {
                    firstTimeStamp = firstTimeStamp > Long.valueOf(nextLine[0]) ? Long.valueOf(nextLine[0]) : firstTimeStamp;
                    break;
                }
            }

            String lastLine;

            ReversedLinesFileReader reverseReader = new ReversedLinesFileReader(file);
            while ((lastLine = reverseReader.readLine()) != null) {
                String[] vals = lastLine.split(",");

                if (vals[0].length() < 13)
                    continue;

                if (vals.length > 0) {
                    lastTimeStamp = lastTimeStamp < Long.valueOf(vals[0]) ? Long.valueOf(vals[0]) : lastTimeStamp;
                    break;
                }
            }
        }

        firstTimeStamp = (firstTimeStamp / 1000L) * 1000L;
    }

    private void setMap(String reportDirName) throws Exception {
        ExecutorService execServ = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        //ExecutorService execServ = Executors.newFixedThreadPool(1);

        List<Future<?>> futList = new ArrayList<>();

        for (File file : files) {
            if (reportDirName.equals("eventstore"))
                futList.add(execServ.submit(new EventFileWorker(file)));
            else
                futList.add(execServ.submit(new ReportFileWorker(file)));
        }
        for (Future f : futList) {
            try {
                f.get();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        execServ.shutdown();
    }

    private PocTaskReport mapToReport() {
        PocTaskReport report = new PocTaskReport();

        List<Long> timePntsList = new ArrayList<>();
        List<Date> dateList = new ArrayList<>();
        List<Long> mainList = new ArrayList<>();
        List<Long> maxList = new ArrayList<>();
        List<Long> opsList = new ArrayList<>();

        for (Map.Entry<Long, ConcurrentLinkedQueue<ReportInterval>> entry : intervalMap.entrySet()) {
            Long timePnt = entry.getKey();
            timePntsList.add(timePnt);
        }
        Collections.sort(timePntsList);

        for (Long timePnt : timePntsList) {
            dateList.add(new Date(timePnt));

            long avgVal = getAvg(intervalMap.get(timePnt));

            mainList.add(avgVal);

            long opsVal = getAvgOps(intervalMap.get(timePnt));

            opsList.add(opsVal);

            maxList.add(getMax(intervalMap.get(timePnt)));
        }

        report.setDateList(dateList);
        report.setMainList(mainList);
        report.setOperCntList(opsList);
        report.setMaxList(maxList);

        XYChart chart = new XYChartBuilder().width(mainWidth).height(600).title(dirName).build();

        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        chart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
        //chart.getStyler().setYAxisLogarithmic(true);

        if (dateList.size() == 0 || mainList.size() == 0) {
            LOG.error("Failed to find data in " + dirName + " directory.");
            return null;
        }

        XYSeries seriesAvg = chart.addSeries("series Avg", dateList, mainList);
        seriesAvg.setMarker(SeriesMarkers.NONE);
        seriesAvg.setLineWidth(1.0F);

        return report;
    }

    private long getAvg(ConcurrentLinkedQueue<ReportInterval> vals) {
        long sum = 0;

        for (ReportInterval val : vals)
            sum = sum + val.getAvg();

        return vals.size() == 0 ? 0L : sum / vals.size();
    }

    private long getAvgOps(ConcurrentLinkedQueue<ReportInterval> vals) {
        long sum = 0;

        for (ReportInterval val : vals)
            sum = sum + val.getOps();

        return vals.size() == 0 ? 0L : sum / vals.size();
    }

    private long getMax(ConcurrentLinkedQueue<ReportInterval> intervals) {
        long max = intervals.peek().getMaxValue();

        for (ReportInterval interval : intervals) {
            if (max < interval.getMaxValue())
                max = interval.getMaxValue();
        }

        return max;
    }

    private long getMin(ConcurrentLinkedQueue<ReportInterval> intervals) {
        long min = intervals.peek().getMinValue();

        for (ReportInterval interval : intervals)
            if (min > interval.getMinValue())
                min = interval.getMinValue();

        return min;
    }

    /**
     *
     * @param dir Directory containing event files.
     * @return {@code Array} of event files.
     */
    private File[] getEventFiles(File dir) {
        File[] res = dir.listFiles(new FileFilter() {
            /**
             * Accepts only '.log' files with more than one line.
             * @param file {@code File} file.
             * @return {@code true} if file name ends with a '.log' and file has more than one line or {@code false}
             * otherwise.
             */
            @Override public boolean accept(File file) {
                // Determine the number of lines in the file.
                int lines = 0;

                try {
                    LineNumberReader lineNumReader = new LineNumberReader(new FileReader(file));

                    lineNumReader.skip(Long.MAX_VALUE);

                    lines = lineNumReader.getLineNumber();

                    lineNumReader.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                String path = file.getAbsolutePath();

                return path.endsWith(".log") && lines > 1;
            }
        });

        return res;
    }

    protected ConcurrentLinkedQueue<ReportInterval> getIntervalList(long timePnt) {
        ConcurrentLinkedQueue<ReportInterval> intervalList = intervalMap.get(timePnt);

        if (intervalList == null) {
            ConcurrentLinkedQueue<ReportInterval> newList = new ConcurrentLinkedQueue<>();

            intervalList = intervalMap.putIfAbsent(timePnt, newList);

            if (intervalList == null)
                intervalList = newList;
        }

        return intervalList;
    }

    private class EventFileWorker implements Runnable {

        private File eventFile;

        private Long interval;

        public EventFileWorker(File eventFile) {
            this.eventFile = eventFile;
            this.interval = 1000L;
        }

        @Override public void run() {
            try {
                CSVReader reader = new CSVReader(new BufferedReader(new FileReader(eventFile.getAbsolutePath())));
                String[] nextLine;

                Long currTimePnt = (firstTimeStamp * 1000L) / 1000L;

                int cntr = 1;
                long dataAccum = 0;

                long ts;
                long val;

                ReportInterval currReportInterval = new ReportInterval(currTimePnt, interval);

                while ((nextLine = reader.readNext()) != null) {
                    if (nextLine[0].contains("Timestamp"))
                        continue;

                    if (nextLine.length < 2)
                        continue;

                    ts = Long.valueOf(nextLine[0]);
                    try {
                        val = Long.valueOf(nextLine[1]);
                    }
                    catch (NumberFormatException ignored) {
                        continue;
                    }

                    if (val <= 0)
                        val = 1L;

                    if (currReportInterval.getMaxValue() == 0) {
                        currReportInterval.setMaxValue(val);
                        currReportInterval.setMinValue(val);
                        currReportInterval.setMaxValueTime(ts);
                    }

                    //ConcurrentLinkedQueue<Long> resList;
                    ConcurrentLinkedQueue<ReportInterval> intervalList;

                    while (ts > currTimePnt + (interval * 2L)) {
                        long nextTimePnt = currTimePnt + interval;

                        ReportInterval nextReportInterval = new ReportInterval(nextTimePnt, interval);

                        nextReportInterval.setAvg(currReportInterval.getAvg());
                        nextReportInterval.setMaxValue(currReportInterval.getMaxValue());
                        nextReportInterval.setMinValue(currReportInterval.getMinValue());
                        nextReportInterval.setOps(0);

                        getIntervalList(currTimePnt).add(currReportInterval);

                        currReportInterval = nextReportInterval;

                        currTimePnt = nextTimePnt;
                    }

                    if (ts < currTimePnt + interval) {
                        dataAccum = dataAccum + val;
                        cntr++;

                        if (currReportInterval.getMaxValue() < val) {
                            currReportInterval.setMaxValue(val);
                            currReportInterval.setMaxValueTime(ts);
                        }

                        if (currReportInterval.getMinValue() > val)
                            currReportInterval.setMinValue(val);
                    }
                    else {
                        dataAccum = dataAccum + val;

                        long avg = dataAccum / cntr;

                        currReportInterval.setAvg(avg);
                        currReportInterval.setOps(cntr);

                        getIntervalList(currTimePnt).add(currReportInterval);

                        currTimePnt = currTimePnt + interval;

                        currReportInterval = new ReportInterval(currTimePnt, interval);
                        dataAccum = 0;
                        cntr = 1;
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    private class ReportFileWorker implements Runnable {

        private File eventFile;

        private Long interval;

        public ReportFileWorker(File eventFile) {
            this.eventFile = eventFile;
            this.interval = 1000L;
        }

        @Override public void run() {
            try {
                CSVReader reader = new CSVReader(new BufferedReader(new FileReader(eventFile.getAbsolutePath())));
                String[] nextLine;

                Long currTimePnt = firstTimeStamp;

                long ts;
                long val;

                ReportInterval currReportInterval = new ReportInterval(currTimePnt, interval);

                while ((nextLine = reader.readNext()) != null) {
                    if (nextLine[0].contains("Timestamp"))
                        continue;

                    if (nextLine.length < 2)
                        continue;

                    ts = Long.valueOf(nextLine[0]);
                    try {
                        val = Long.valueOf(nextLine[1]);
                    }
                    catch (NumberFormatException ignored) {
                        continue;
                    }

                    long roundedTS = (ts / 1000L) * 1000L;

                    currReportInterval.setAvg(val);

                    getIntervalList(roundedTS).add(currReportInterval);

                    currReportInterval = new ReportInterval(roundedTS + 1000L, interval);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

}
