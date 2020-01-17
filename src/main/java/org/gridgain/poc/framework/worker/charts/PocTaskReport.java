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

import java.util.Date;
import java.util.List;
import java.util.Map;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.style.Styler;

/**
 * Created by oostanin on 06.12.17.
 */
public class PocTaskReport {

    private String taskType;
    private String taskName;
    private String taskID;
    private String reportDirName;

    private XYChart mainChart;

    private Map<Object, Object> propMap;

    private long firstTimeStamp;

    private long lastTimeStamp;

    private List<Date> dateList;
    private List<Long> mainList;
    private List<Long> maxList;
    private List<Long> minList;
    private List<Long> operCntList;

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public void setMainChart(XYChart mainChart) {
        this.mainChart = mainChart;
    }

    public long getFirstTimeStamp() {
        return firstTimeStamp;
    }

    public void setFirstTimeStamp(long firstTimeStamp) {
        this.firstTimeStamp = firstTimeStamp;
    }

    public long getLastTimeStamp() {
        return lastTimeStamp;
    }

    public void setLastTimeStamp(long lastTimeStamp) {
        this.lastTimeStamp = lastTimeStamp;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public List<Date> getDateList() {
        return dateList;
    }

    public void setDateList(List<Date> dateList) {
        this.dateList = dateList;
    }

    public List<Long> getMainList() {
        return mainList;
    }

    public void setMainList(List<Long> mainList) {
        this.mainList = mainList;
    }

    public List<Long> getMaxList() {
        return maxList;
    }

    public void setMaxList(List<Long> maxList) {
        this.maxList = maxList;
    }

    public List<Long> getMinList() {
        return minList;
    }

    public void setMinList(List<Long> minList) {
        this.minList = minList;
    }

    public List<Long> getOperCntList() {
        return operCntList;
    }

    public void setOperCntList(List<Long> operCntList) {
        this.operCntList = operCntList;
    }

    public String getReportDirName() {
        return reportDirName;
    }

    public void setReportDirName(String reportDirName) {
        this.reportDirName = reportDirName;
    }

    public Map<Object, Object> getPropMap() {
        return propMap;
    }

    public void setPropMap(Map<Object, Object> propMap) {
        this.propMap = propMap;
    }

    public XYChart getMainChart() {
        mainChart = new XYChartBuilder().width(mainList.size()).height(600).title(taskName).build();

        mainChart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
        mainChart.getStyler().setLegendLayout(Styler.LegendLayout.Horizontal);
        //mainChart.getStyler().setYAxisLogarithmic(true);

        return mainChart;
    }

}
