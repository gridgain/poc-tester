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

package org.apache.ignite.scenario.internal.utils;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.scenario.internal.PocTesterArguments;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.hms;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;

/**
 * Class for printing client node related help..
 */
public class StartClientsWorker extends StartNodeWorker {
    /** */
    public static void main(String[] args) {
        new StartClientsWorker().work(args);
    }

    /**
     * Print help.
     */
    protected void printHelp(){
        System.out.println(" Start client nodes.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" start-clients.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -tp   || --taskProperties  <path to task property file>");
        System.out.println(" -jh   || --definedJavaHome <path to java home on remote hosts>");
        System.out.println();
        System.exit(0);
    }
}


