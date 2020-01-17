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

import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;

/**
 * Created by oostanin on 13.12.17.
 */
public class DstatLogWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(DstatLogWorker.class.getName());

    private File mainDir;

    private PocTesterArguments args;

    private List<File> fileList;

    private Map<String, Set<Long>> cachePartMap;
    private Map<String, List<File>> servDstatMap;

    private Set<String> logSet;
    private List<String> misValList;

    /** */
    private List<String> colNameList;

    private Set<String> includedCols;

    private long firstTimeStamp;
    private long lastTimeStamp;

    public DstatLogWorker(PocTesterArguments args, File mainDir) {
        this.args = args;

        this.mainDir = mainDir;

        cachePartMap = new HashMap<>();

        servDstatMap = new HashMap<>();

        logSet = new TreeSet<>();

        misValList = new ArrayList<>();

        setIncludedCols();
    }

    private void setIncludedCols(){
        includedCols = new HashSet<>();

        includedCols.add("total-cpu-usage-sys");
        includedCols.add("total-cpu-usage-usr");
        includedCols.add("dsk-total-writ");
        includedCols.add("dsk-total-read");
        includedCols.add("memory-usage-used");
        includedCols.add("net-total-recv");
        includedCols.add("net-total-send");
    }

    public static void main(String[] args) {
        DstatLogWorker creator = new DstatLogWorker(null, new File("/home/oostanin/gg-qa/gg-qa/poc-tester/target/assembly/log-2018-03-17-16-02-16"));

        creator.setServDstatMap();
    }

    /** */
    private void setColumnNameList() throws IOException {
        colNameList = new ArrayList<>();

        for (String hostIP : servDstatMap.keySet()) {
            for (File dstatLogFile : servDstatMap.get(hostIP)) {
                BufferedReader reader = new BufferedReader(new FileReader(dstatLogFile));

                String line;

                String baseLine = null;

                String colLine = null;

                while ((line = reader.readLine()) != null && (baseLine == null || colLine == null)) {
                    if (line.startsWith("\"epoch\"") && baseLine != null)
                        colLine = line;

                    if (line.startsWith("\"epoch\"") && baseLine == null)
                        baseLine = line;

                }

                if (baseLine != null && colLine != null) {
                    String[] baseArr = baseLine.split(",");

                    String[] colArr = colLine.split(",");

                    String pref = "";

//                    for (int i = 0; i < baseArr.length; i++) {
                    for (int i = 0; i <= 16; i++) {
                        if (!baseArr[i].equals(""))
                            pref = baseArr[i].replace("\"", "").replace(" ", "-").replace("/", "-");

                        String colName = String.format("%s-%s", pref, colArr[i].replace("\"", ""));

                        colNameList.add(colName);
                    }

                    return;
                }
            }
        }
    }

    public void setServDstatMap() {
        setDstatLogMap();

        try {
            setColumnNameList();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        final File dstatChartDir = Paths.get(mainDir.getAbsolutePath(), "dstat-charts").toFile();

        if (!dstatChartDir.exists())
            dstatChartDir.mkdirs();


        ExecutorService execServ = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        List<Future<?>> futList = new ArrayList<>();

        for (final String hostIP : servDstatMap.keySet()) {
            if(args != null && !PocTesterUtils.getHostList(args.getServerHosts(), true).contains(hostIP))
                continue;

            futList.add(execServ.submit(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    File hostChartDir = Paths.get(dstatChartDir.getAbsolutePath(), hostIP).toFile();

                    List<XYChart> chartList = new ArrayList<>();

                    if (!hostChartDir.exists())
                        hostChartDir.mkdirs();

                    List<Date> dateList = new ArrayList<>();

                    List<List<Double>> dataColList = new ArrayList<>();

                    for (int i = 0; i < colNameList.size(); i ++) {
                        String chartName = colNameList.get(i);

                        if(!includedCols.contains(chartName)) {
                            chartList.add(i, null);

                            dataColList.add(i, null);

                            continue;
                        }

                        List<Double> valList = new ArrayList<>();

                        dataColList.add(i, valList);

                        XYChart colChart = new XYChartBuilder().width(1800).height(400).
                            title(String.format("%s-%s", hostIP, chartName)).build();

                        colChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
                        colChart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
                        colChart.getStyler().setYAxisLogarithmic(true);

                        chartList.add(i, colChart);
                    }

                    List<Long> timeStampList = new ArrayList<>();

                    for (File dstatLogFile : servDstatMap.get(hostIP)) {
                        try {
                            BufferedReader reader = new BufferedReader(new FileReader(dstatLogFile));

                            String line;

                            Double discWritePrevVal = 1D;

                            int cntr = 0;

                            while ((line = reader.readLine()) != null) {
                                if (!line.startsWith("1"))
                                    continue;

                                String[] vals = line.split(",");

                                for (int i = 0; i < dataColList.size(); i++) {
                                    if (i == 0) {
                                        Long ts = Long.valueOf(vals[i].replace(".", "")) / 1000L * 1000L;

                                        timeStampList.add(ts);

                                        dateList.add(new Date(ts));
                                    }

                                    List<Double> dataList = dataColList.get(i);

                                    if (dataList == null)
                                        continue;

                                    Double val = Double.valueOf(vals[i]);

                                    if (i > 0 && i < colNameList.size() && colNameList.get(i).equals("dsk-total-writ")) {
                                        if (val != 0) {
                                            dataList.add(val);

                                            discWritePrevVal = val;
                                        }
                                        else
                                            dataList.add(discWritePrevVal);

                                    }
                                    else {
                                        if (val == 0)
                                            val = val + 1;

                                        dataList.add(val + 1);
                                    }
                                }

                                if(cntr != 0 && cntr % 1800 == 0)
                                    makeShortCharts(cntr, hostChartDir, dateList, dataColList, timeStampList);

                                cntr++;
                            }

                            makeShortCharts(cntr, hostChartDir, dateList, dataColList, timeStampList);
                        }
                        catch (IOException e) {
                            LOG.error(String.format("Failed to read log file %s", dstatLogFile.getAbsolutePath()), e);
                        }
                    }

                    for(int i = 0; i < chartList.size(); i++){
                        XYChart chart = chartList.get(i);

                        if (chart == null)
                            continue;

                        XYSeries series = chart.addSeries("main", dateList, dataColList.get(i));

                        series.setMarker(SeriesMarkers.NONE);
                        series.setLineColor(Color.BLUE);
                        series.setLineWidth(1.0F);

                        String fileName = hostChartDir + File.separator + chart.getTitle();
                        try {
                            BitmapEncoder.saveBitmap(chart, fileName, BitmapEncoder.BitmapFormat.PNG);
                        }
                        catch (IOException e) {
                            LOG.error("Failed to save chart", e.getMessage());
                        }
                    }

                    return null;
                }
            }));
        }

        for (Future f : futList) {
            try {
                f.get();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        execServ.shutdown();
    }

    private void makeShortCharts(int cntr, File hostChartDir, List<Date> dateList, List<List<Double>> dataColList,
        List<Long> timeStampList){

        int shift = cntr > 1800 ? 1800 : cntr;

        int beginIdx = cntr - shift;

        int endIdx = dateList.size() - 1;

        if(dateList.isEmpty())
            return;

        String shortChartDirName = dateTime(dateList.get(beginIdx));

        File shortChartDir = Paths.get(hostChartDir.getAbsolutePath(), shortChartDirName).toFile();

        List<XYChart> shortChartList = new ArrayList<>();

        List<Date> shortDateList = dateList.subList(beginIdx, endIdx);

        if (!shortChartDir.exists())
            shortChartDir.mkdirs();

        for (int i = 0; i < colNameList.size(); i ++) {
            String chartName = colNameList.get(i);

            if(!includedCols.contains(chartName))
                continue;

            XYChart shortChart = new XYChartBuilder().width(1800).height(400).
                title(String.format("%s-%s", shortChartDirName, chartName)).build();

            shortChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
            shortChart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
            shortChart.getStyler().setYAxisLogarithmic(true);

            shortChartList.add(shortChart);

            List<Double> shortValList = dataColList.get(i).subList(beginIdx, endIdx);

            XYSeries series = shortChart.addSeries("main", shortDateList, shortValList);

            series.setMarker(SeriesMarkers.NONE);
            series.setLineColor(Color.BLUE);
            series.setLineWidth(1.0F);

            String fileName = shortChartDir.getAbsolutePath() + File.separator + shortChart.getTitle();
            try {
                BitmapEncoder.saveBitmap(shortChart, fileName, BitmapEncoder.BitmapFormat.PNG);
            }
            catch (IOException e) {
                LOG.error("Failed to save chart", e.getMessage());
            }
        }
    }

    private void parse(File file) throws IOException {
        if (file.getName().startsWith("gc-") || file.getName().startsWith("iostat"))
            return;

        if (file.getName().endsWith(".gz")) {
//            File tempDecompressedFile = new File(file.getAbsolutePath().replace(".gz", ".temp"));
            File tempDecompressedFile = new File(file.getAbsolutePath().replace(".gz", ""));

            unGunzipFile(file.getAbsolutePath(), tempDecompressedFile.getAbsolutePath());

            setMap(tempDecompressedFile);

//            tempDecompressedFile.delete();
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
                if (misValList.size() < 10000)
                    misValList.add(String.format("%s   [file: %s]", line, file.getName()));

                int dotInd = line.indexOf('.');
                int start = line.lastIndexOf("cachepoc");

                int failStart = line.lastIndexOf("Failed to find");

                int inStart = line.lastIndexOf(" in cache ");

                String cacheName = null;

                try {
                    cacheName = line.substring(start, dotInd);
                }
                catch (StringIndexOutOfBoundsException e) {
                    System.out.println(String.format("%s : %d : %d", line, start, dotInd));

                    e.printStackTrace();

                    System.exit(1);
                }

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
        }
    }

    /**
     *
     */
    private void setDstatLogMap() {
        Path statDir = Paths.get(mainDir.getAbsolutePath(), "stats");

        if (!statDir.toFile().exists()){
            LOG.warn(String.format("Failed to find stats directory in %s", mainDir.getAbsolutePath()));

            return;
        }

        final File[] hostDirs = PocTesterUtils.getSubDirs(statDir, null);

        if (hostDirs == null || hostDirs.length == 0){
            LOG.warn(String.format("Failed to find host directories in %s", statDir.toFile().getAbsolutePath()));

            return;
        }

        for (File hostDir : hostDirs) {
            File dstatDir = Paths.get(hostDir.getAbsolutePath(), "dstat").toFile();

            if (!dstatDir.exists()){
                LOG.warn(String.format("Failed to find dstat directory in %s", hostDir.getAbsolutePath()));

                continue;
            }

            File[] dstatLogs = dstatDir.listFiles();

            if (dstatLogs == null || dstatLogs.length == 0) {
                LOG.error(String.format("Failed to get log files from directory %s", dstatDir));

                continue;
            }

            servDstatMap.put(hostDir.getName(), Arrays.asList(dstatLogs));
        }
    }

    private Long getTimeStamp(String line) throws ParseException {
        String dateTime = line.substring(1, 24);

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

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
