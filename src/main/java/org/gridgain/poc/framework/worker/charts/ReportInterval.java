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

/**
 * Created by oostanin on 11.12.17.
 */
public class ReportInterval {

    private long startPoint;
    private long finalPoint;

    private long duration;

    private long maxValue;
    private long minValue;
    private long avg;
    private long ops;

    private long maxValueTime;

    public ReportInterval(long startPoint, long duration) {
        this.startPoint = startPoint;
        this.duration = duration;
        this.maxValueTime = startPoint;
    }

    public long getStartPoint() {
        return startPoint;
    }

    public void setStartPoint(long startPoint) {
        this.startPoint = startPoint;
    }

    public long getFinalPoint() {
        return finalPoint;
    }

    public void setFinalPoint(long finalPoint) {
        this.finalPoint = finalPoint;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
    }

    public long getMinValue() {
        return minValue;
    }

    public void setMinValue(long minValue) {
        this.minValue = minValue;
    }

    public long getAvg() {
        return avg;
    }

    public void setAvg(long avg) {
        this.avg = avg;
    }

    public long getOps() {
        return ops;
    }

    public void setOps(long ops) {
        this.ops = ops;
    }

    public long getMaxValueTime() {
        return maxValueTime;
    }

    public void setMaxValueTime(long maxValueTime) {
        this.maxValueTime = maxValueTime;
    }
}
