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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by oostanin on 07.12.17.
 */
public class JsonWorker {

    private File mainDir;

    private ConcurrentHashMap<String, ConcurrentLinkedQueue<File>> taskMap;

    public JsonWorker(File mainDir) {
        this.mainDir = mainDir;
    }

    public void convert() {
        List<File> fileList = new ArrayList<>();
        walk(mainDir, fileList);

        ExecutorService execServ = Executors.newFixedThreadPool(1);

        List<Future<?>> futList = new ArrayList<>();

        for (File file : fileList)
                futList.add(execServ.submit(new FileWorker(file)));


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

    public void walk(File path, List<File> fileList) {

        File root = path;
        File[] list = root.listFiles();

        if (list == null)
            return;

        for (File f : list) {
            if (f.isDirectory())
                walk(f, fileList);

            else
                if(f.getParent().contains("eventstore") || f.getParent().contains("reports"))
                    if(f.getAbsolutePath().endsWith(".log"))
                        fileList.add(f);

        }
    }

    private class FileWorker implements Callable<Map<Long, List<Long>>> {

        private File eventFile;

        private Long interval;

        private Long firstTimeStamp;

        private Map<Long, List<Long>> toJsonMap;

        public FileWorker(File eventFile) {
            this.eventFile = eventFile;
            this.interval = 1000L;
            this.firstTimeStamp = 0L;
        }

        @Override public Map<Long, List<Long>> call() {
            try {
                setFirstTimeStamp(eventFile);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            toJsonMap = new HashMap<>();

            try {
                CSVReader reader = new CSVReader(new BufferedReader(new FileReader(eventFile.getAbsolutePath())));
                String[] nextLine;

                Long currentTimePoint = firstTimeStamp;

                long cntr = 1L;
                long dataAccum = 0L;

                long ts;
                long val;

                while ((nextLine = reader.readNext()) != null) {
                    if (nextLine[0].contains("Timestamp"))
                        continue;

                    if (nextLine.length < 2)
                        continue;

                    ts = Long.valueOf(nextLine[0]);
                    val = Long.valueOf(nextLine[1]);


                    List<Long> resList = new ArrayList<>();

                    if (eventFile.getName().contains("report")){
                        resList.add(val);
                        toJsonMap.put(ts, resList);
                        continue;
                    }

                    if (ts < currentTimePoint + interval) {
                        dataAccum = dataAccum + val;
                        cntr++;
                    }
                    else {
                        dataAccum = dataAccum + val;

                        resList.add(dataAccum / cntr);
                        resList.add(cntr);

                        toJsonMap.put(currentTimePoint, resList);

                        currentTimePoint = ts;
                        dataAccum = 0;
                        cntr = 1;
                    }

                }

                String jsonName = eventFile.getAbsolutePath().replace(".log", ".json");

                dumpRes(jsonName, toJsonMap);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            return toJsonMap;
        }

        private void setFirstTimeStamp(File file) throws Exception {
            CSVReader reader = new CSVReader(new BufferedReader(new FileReader(file.getAbsolutePath())));
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null && firstTimeStamp == 0) {
                if (nextLine[0].contains("Timestamp"))
                    continue;

                firstTimeStamp = Long.valueOf(nextLine[0]);
            }
        }

        private void dumpRes(final String path, final Map<Long, List<Long>> map) throws IOException {

            final ObjectMapper mapper = new ObjectMapper();

            final PrintWriter writer = new PrintWriter(new FileOutputStream(new File(path)));

            writer.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));

            writer.close();
        }
    }
}
