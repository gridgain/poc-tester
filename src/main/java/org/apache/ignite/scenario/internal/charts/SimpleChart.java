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
import java.util.List;
import java.util.Map;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.utils.AbstractWorker;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYSeries;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printw;

/**
 * Millisecond Scale
 * <p>
 * Demonstrates the following:
 * <ul>
 * <li>Millisecond Scale
 * <li>LegendPosition.OutsideS
 * <li>Two YAxis Groups - both on left
 */
public class SimpleChart extends AbstractWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(SimpleChart.class.getName());

    /**
     * Main method.
     *
     * @param args Arguments.
     */
    public static void main(String[] args) {
        new SimpleChart().create(args);
    }

    /**
     * Print help.
     */
    @Override protected void printHelp(){
        System.out.println(" Creates reports and simple charts.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" simple-chart.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        System.out.println();
        System.out.println(" -rd || --resDir          Result directory. If not defined newest log directory will be used.");
        System.out.println();
        System.exit(0);
    }

    /**
     * Creates reports and simple charts from result directory.
     *
     * @param argsArr Arguments.
     */
    protected void create(String[] argsArr){
        args = new PocTesterArguments();
        args.setArgsFromCmdLine(argsArr);
        args.setArgsFromStartProps();

        if (args.isHelp())
            printHelp();

        String resDirPath;

        if (args.getResDir() != null )
            resDirPath = args.getResDir();
        else {
            printw("Result directory is not defined. Will try to create report for the newest log directory.");

            resDirPath = getNewestLogDir();
        }

        if(resDirPath == null) {
            printer("Result directory is not found.");

            System.exit(1);
        }

        File resDir = new File(resDirPath);

        File reportDir = new File(resDirPath + File.separator + "report");

        if(!reportDir.exists())
            reportDir.mkdir();

        println("Creating charts from directory " + resDir);
        System.out.println();

        List<XYChart> charts = new DataAssembler(resDir).getChartList();

        for(XYChart chart : charts){
            Map<String, XYSeries> series = chart.getSeriesMap();

            String fileName = resDir + File.separator + chart.getTitle();
            try {
                BitmapEncoder.saveBitmap(chart, fileName, BitmapEncoder.BitmapFormat.PNG);
            }
            catch (IOException e) {
                LOG.error("Failed to save chart", e.getMessage());
            }
        }

        new DstatLogWorker(args, resDir).setServDstatMap();
    }
}
