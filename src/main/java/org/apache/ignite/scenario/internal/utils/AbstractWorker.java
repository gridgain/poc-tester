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

import com.beust.jcommander.ParameterException;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.printer;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;

/**
 * Created by oostanin on 03.01.18.
 */
public abstract class AbstractWorker {
    /** */
    protected static final Random COMMON_RANDOM = new Random();

    /** */
    protected final Logger LOG = LogManager.getLogger(this.getClass().getName());

    /** */
    protected PocTesterArguments args;

    /** */
    protected String locHome;

    /** */
    protected String rmtHome;

    /** */
    protected List<String> linesToLook;

    /** */
    protected final Map<String, Semaphore> semMap = new HashMap<>();


    public AbstractWorker() {
        locHome = System.getProperty("pocTesterHome");

        linesToLook = new ArrayList<>();

        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);

        File file = new File(String.format("%s/config/cluster/log4j2.xml", locHome));

        ctx.setConfigLocation(file.toURI());
    }

    /**
     *
     */
    protected abstract void printHelp();

    /**
     *
     */
    public static void commonHelp() {
        System.out.println(" -h    || --help            Print help.");
        System.out.println(" -sp   || --startProperties <file>  Relative path to the common property file. You can find example in 'config/example-common.properties'.");
        System.out.println(" -s    || --serverHosts     <comma separated list of server node IP addresses e.g. 172.1.0.1,172.1.0.2 or ! (exclamation point) to ignore any server host addresses from start property file.>");
        System.out.println(" -c    || --clientHosts     <comma separated list of client node IP addresses e.g. 172.1.0.1,172.1.0.2 or ! (exclamation point) to ignore any client host addresses from start property file.>");
        System.out.println(" -u    || --user            <user name for ssh connection>");
        System.out.println(" -kp   || --keyPath         <path to key file for ssh connection>");
    }

    /**
     * Find newest log directory.
     * @return {@code String} Path to newest log directory if any of log directories exists or {@code null} if no log
     * directories are present.
     */
    protected String getNewestLogDir(){
        List<String> dirList = new ArrayList<>();

        File[] fileArr = new File(locHome).listFiles();

        for (File file : fileArr){
            File[] childArr = file.listFiles();

            if(childArr == null)
                continue;

            List<String> childList =  new ArrayList<>();

            for(File child : childArr)
                childList.add(child.getName());

            if(file.getName().startsWith("log-") && childList.contains("clients"))
                dirList.add(file.getAbsolutePath());
        }

        Collections.sort(dirList);

        if(!dirList.isEmpty())
            return dirList.get(dirList.size() - 1);

        printer("Failed to find newest log directory.");

        return null;
    }
    /**
     * Checks if host IP is 'localhost' or '127.0.0.1';
     *
     * @param host {@code String} hos IP;
     * @return {@code true} if host IP is 'localhost' or '127.0.0.1' or {@code false} otherwise.
     */
    protected boolean isLocalHost(String host) {
        return (host.equals("127.0.0.1") || host.equals("localhost"));
    }
}
