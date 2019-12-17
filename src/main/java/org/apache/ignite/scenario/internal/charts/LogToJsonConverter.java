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
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by oostanin on 07.12.17.
 */
public class LogToJsonConverter {
    private static final Logger LOG = LogManager.getLogger(LogToJsonConverter.class.getName());


    public static void main(String[] args) {
        if (args.length == 0)
            LOG.error("Result directory is not defined");

        File resultDir = new File(args[0]);
        //File resultDir = new File("/home/oostanin/gg-qa/gg-qa/poc-tester/target/assembly/logs-2017-12-06-18-35-18");

        new JsonWorker(resultDir).convert();
    }





}
