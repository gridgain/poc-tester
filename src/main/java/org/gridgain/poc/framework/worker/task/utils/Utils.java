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

package org.gridgain.poc.framework.worker.task.utils;

import org.apache.commons.text.RandomStringGenerator;
import org.apache.ignite.Ignite;
import org.gridgain.poc.framework.starter.TaskStarter;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;

/** */
public class Utils {
    /** */
    private static final String[] EMPTY_ARGS = new String[0];

    /** */
    private static final ThreadLocal<RandomStringGenerator> _generator = new ThreadLocal<>();

    /** */
    private static int nodes = 1;

    /** */
    private static String cfg = "config/multicast-remote-server-config.xml";

    private static Ignite igniteServer;

    public static void main(String[] args) throws Exception {
        String params = "[2018-04-16 15:30:46,803][INFO ][Node-starter-172.25.1.32-[15:30:46] ] " +
            "LOG_FILE_NAME=/storage/hdd/oostanin/poc-tester/log/task-2018-04-16-15-30-33-JdbcPlainInsertTableTask/poc-tester-client-172.25.1.32-id-0-2018-04-16-15-30-33.log " +
            "POC_TESTER_HOME=/storage/hdd/oostanin/poc-tester  /usr/lib/jvm/java-8-oracle/bin/java    " +
            "-ea   -XX:+PrintGCDetails -verbose:gc -XX:+UseG1GC -XX:+DisableExplicitGC -XX:+PrintGCDateStamps " +
            "-Djava.net.preferIPv4Stack=true   " +
            "-Xloggc:/storage/hdd/oostanin/poc-tester//log/task-2018-04-16-15-30-33-JdbcPlainInsertTableTask/gc-poc-tester-client-172.25.1.32-id-0-2018-04-16-15-30-33.log    " +
            "-Xms6g -Xmx6g  -XX:ErrorFile=/storage/hdd/oostanin/poc-tester/log/task-2018-04-16-15-30-33-JdbcPlainInsertTableTask/crash-dump-2018-04-16-15-30-45.log " +
            "-DpocLogFileName=/storage/hdd/oostanin/poc-tester/log/task-2018-04-16-15-30-33-JdbcPlainInsertTableTask/poc-tester-client-172.25.1.32-id-0-2018-04-16-15-30-33.log " +
            "-DCONSISTENT_ID=poc-tester-client-172.25.1.32-id-0 " +
            "-DpocTesterHome=/storage/hdd/oostanin/poc-tester " +
            "-DclientDirName=task-2018-04-16-15-30-33-JdbcPlainInsertTableTask " +
            "-DipInCluster=172.25.1.32 " +
            "-cp /storage/hdd/oostanin/poc-tester/libs/*:/storage/hdd/oostanin/poc-tester/task-2018-04-16-15-30-33-JdbcPlainInsertTableTask/libs/* " +
            "org.gridgain.poc.framework.starter.TaskStarter  " +
            "-sp /storage/hdd/oostanin/poc-tester/config/common.properties " +
            "-tp /storage/hdd/oostanin/poc-tester/task-2018-04-16-15-30-33-JdbcPlainInsertTableTask/config/jdbc/qaoo-plain-insert.properties " +
            "-ccfg /storage/hdd/oostanin/poc-tester/task-2018-04-16-15-30-33-JdbcPlainInsertTableTask/config/cluster/prepared-vmipfinder-remote-client-config.xml " +
            "-cntr 1 -ttl 3 \n";

        String taskCls = "JdbcPlainInsertTableTask";

        String clientDirName = String.format("task-%s-%s", dateTime(), taskCls);

        System.setProperty("pocTesterHome", "/home/oostanin/gg-qa/poc-tester/target/assembly");
        System.setProperty("clientDirName", "/home/oostanin/gg-qa/poc-tester/target/assembly");

        String[] paramArr = new String[]{"-tp", "/home/oostanin/gg-qa/poc-tester/config/jdbc/qaoo-tx.properties"};

        TaskStarter.main(paramArr);

//        LoadCacheTask.main(EMPTY_ARGS);
//        PutGetCacheTask.main(EMPTY_ARGS);
//        TxCacheTask.main(EMPTY_ARGS);
//
//        System.out.println("end");
//
//        igniteServer.close();
    }

    /** */
    static RandomStringGenerator literalsGenerator() {
        RandomStringGenerator generator = _generator.get();
        if (generator == null) {
            generator = new RandomStringGenerator.Builder().withinRange('a', 'z')
                .build();
            _generator.set(generator);
        }
        return generator;
    }

    /**
     * @param len results string length.
     * @return random literals string.
     */
    public static String randomString(int len) {
        return literalsGenerator().generate(len);
    }
}