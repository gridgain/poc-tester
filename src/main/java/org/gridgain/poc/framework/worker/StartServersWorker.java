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

package org.gridgain.poc.framework.worker;

/**
 * Class for printing server node related help..
 */
public class StartServersWorker extends StartNodeWorker {
    /** */
    public static void main(String[] args) {
        new StartServersWorker().work(args);
    }

    /**
     * Print help.
     */
    protected void printHelp() {
        System.out.println(" Start server nodes.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" start-servers.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        commonHelp();
        System.out.println(" -jh   || --definedJavaHome <path to java home on remote hosts>");
        System.out.println(" -wp   || --walPath         <defined WAL directory>");
        System.out.println(" -wa   || --walArch         <defined WAL archive director>");
        System.out.println(" -st   || --startThreads    <number of start threads>");
        System.out.println("                            option allows you to define number of threads which will " +
            "start Ignite server nodes in parallel. For example \n" +
            "                            -st 4 \n" +
            "                            will allow 4 Ignite nodes to start at the same time.");
        System.out.println(" -fs   || --fastStart       <initial consistent id suffix>");
        System.out.println("                            option allows you to define nodes id to avoid consistent id " +
            "duplication on the cluster start. For example \n" +
            "                            -fs 12 \n" +
            "                            will start first defined node with id ‘poc-tester-server-172.25.1.30-id-12’, " +
            "second node with id ‘poc-tester-server-172.25.1.30-id-13’ and so on.");
        System.out.println();
    }
}


