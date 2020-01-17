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
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Created by oostanin on 02.12.17.
 */
public class Reader {

    public IgniteBiTuple<ArrayList<Date>, ArrayList<Long>> getData() throws Exception {
        IgniteBiTuple<ArrayList<Date>, ArrayList<Long>> res = new IgniteBiTuple<>();

        ArrayList<Date> timeStamps = new ArrayList<>();
        ArrayList<Long> values = new ArrayList<>();

        CSVReader reader = new CSVReader(new BufferedReader(new FileReader("./sample-tx-events.log")));
        String[] nextLine;

        int cntr = 500;

        while ((nextLine = reader.readNext()) != null && cntr > 0) {
            // nextLine[] is an array of values from the line
            //System.out.println(nextLine[0] + nextLine[1] + "etc...");
            if (!nextLine[0].contains("stamp")) {
                timeStamps.add(new Date(Long.valueOf(nextLine[0])));
                values.add(Long.valueOf(nextLine[1]));
            }

            cntr--;
        }

        res.set1(timeStamps);
        res.set2(values);

        return res;
    }

    public IgniteBiTuple<ArrayList<Date>, ArrayList<Long>> getSegmentedData(long interval,
        long startTime) throws Exception {
        IgniteBiTuple<ArrayList<Date>, ArrayList<Long>> res = new IgniteBiTuple<>();

        ArrayList<Date> timeStamps = new ArrayList<>();
        ArrayList<Long> values = new ArrayList<>();

        CSVReader reader = new CSVReader(new BufferedReader(new FileReader("./sample-tx-events.log")));
        String[] nextLine;

        int cntr = 1;
        long dataAccum = 0;

        long ts;
        long val;

        //long currTimePoint = startTime;

        while ((nextLine = reader.readNext()) != null) {
            if (nextLine[0].contains("Timestamp"))
                continue;

            ts = Long.valueOf(nextLine[0]);
            val = Long.valueOf(nextLine[1]);

            long nextTimePnt = startTime + interval * 2;

            if(nextTimePnt < ts){
                timeStamps.add(new Date(startTime + 1));
                values.add(0L);
                timeStamps.add(new Date(ts - 1));
                values.add(0L);
            }

            if (startTime + interval > ts) {
                dataAccum = dataAccum + val;
                cntr++;
            }
            else {
                timeStamps.add(new Date(ts));
                values.add(dataAccum / cntr);
                startTime = ts;
                dataAccum = 0;
                cntr=1;
            }

        }

        res.set1(timeStamps);
        res.set2(values);

        return res;
    }
}
