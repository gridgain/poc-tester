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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;

/**
 * Created by oostanin on 18.12.17.
 */
public class PocChartBuilder {

    private long firstTimeStamp;
    private long lastTimeStamp;

    public List<XYChart> buildCharts(List<PocTaskReport> reports){
        setTimeStamps(reports);

        List<Date> timeList = new ArrayList<>();
        List<Long> timeDots = new ArrayList<>();

        timeList.add(new Date(firstTimeStamp));
        timeList.add(new Date(lastTimeStamp));

        timeDots.add(1L);
        timeDots.add(1L);


        List<XYChart> res = new ArrayList<>();

        for(PocTaskReport report : reports){
            String title = report.getTaskName();

            XYChart mainChart = new XYChartBuilder().width(3600).height(400).
                title(report.getTaskID()).build();

            mainChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
            mainChart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);

            if(!report.getReportDirName().contains("snapshot"))
                mainChart.getStyler().setYAxisLogarithmic(true);

            List<Long> toChart = new ArrayList<>(report.getMainList().size());

            for(Long val : report.getMainList()) {
                if (val < 0L) {
                    System.out.println("Val is less then zero: " + val);

                    val = 1L;
                }
                val = val +1;

                toChart.add(val);
            }

            XYSeries seriesMain = mainChart.addSeries("Average", report.getDateList(), toChart);
            seriesMain.setMarker(SeriesMarkers.NONE);
            seriesMain.setLineWidth(1.0F);

//            XYSeries seriesTime = mainChart.addSeries(" ", timeList, timeDots);
//            seriesTime.setMarker(SeriesMarkers.NONE);
//            seriesTime.setLineColor(Color.WHITE);
//            seriesTime.setLineWidth(0.1F);

//            if(report.getMaxList() != null) {
//
//                XYSeries seriesMax = mainChart.addSeries("series Max",  report.getDateList(), report.getMaxList());
//                seriesMax.setMarker(SeriesMarkers.NONE);
//                seriesMax.setLineWidth(0.5F);
//
//            }

//            if (!report.getTaskType().equals("snapshot"))
//                addSnapshotSerie(mainChart, report, reports);

            res.add(mainChart);

        }
        return res;
    }

    private void addSnapshotSerie(XYChart mainChart, PocTaskReport mainReport, List<PocTaskReport> reports){
        for(PocTaskReport taskReport : reports){

            if(taskReport.getTaskType().equals("snapshot")){
                List<Date> dateList = new ArrayList<>();
                List<Long> dotList = new ArrayList<>();

                for(int i = 0; i < taskReport.getDateList().size(); i++){
                    long timePnt = taskReport.getDateList().get(i).getTime();

                    if(timePnt > mainReport.getFirstTimeStamp() && timePnt < mainReport.getLastTimeStamp()){
                        dateList.add(taskReport.getDateList().get(i));
                        dotList.add(taskReport.getMainList().get(i));
                    }
                }

                if(dateList.size() > 0){
                    XYSeries seriesSnap = mainChart.addSeries("Snapshots",  dateList, dotList);
                    seriesSnap.setMarker(SeriesMarkers.NONE);
                    seriesSnap.setLineColor(Color.GRAY);
                    seriesSnap.setLineWidth(1.5F);
                }
            }
        }
    }

    private void setTimeStamps(List<PocTaskReport> reports){
        firstTimeStamp = Long.MAX_VALUE;
        lastTimeStamp = 0L;

        for(PocTaskReport report : reports ){
            if(report.getFirstTimeStamp() < firstTimeStamp)
                firstTimeStamp = report.getFirstTimeStamp();

            if(report.getLastTimeStamp() > lastTimeStamp)
                lastTimeStamp = report.getLastTimeStamp();
        }
    }
}
