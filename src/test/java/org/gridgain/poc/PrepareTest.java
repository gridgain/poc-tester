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

package org.gridgain.poc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.gridgain.poc.framework.worker.PrepareWorker;
import org.junit.After;
import org.junit.Test;

import static org.gridgain.poc.framework.utils.PocTesterUtils.printw;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
public class PrepareTest extends AbstractCommonTest {
    /**
     * @throws Exception if failed.
     */
    @Test
    public void prepareUser() throws Exception {
        PrepareWorker.main(new String[] {"-s", "127.0.0.1..5x2", "-u", "ivanov"});

        Properties props = getProperties();

        assertNotNull(props.getProperty("USER"));
        assertEquals(props.getProperty("USER"),"ivanov");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void prepareKeyPath() throws Exception {
        PrepareWorker.main(new String[] {"-s", "127.0.0.1..5x2", "-kp", "/home/user/.ssh/id_rsa"});

        Properties props = getProperties();

        assertNotNull(props.getProperty("SSH_KEY_PATH"));
        assertEquals(props.getProperty("SSH_KEY_PATH"),"/home/user/.ssh/id_rsa");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void prepareServClientList() throws Exception {
        PrepareWorker.main(new String[] {"-s", "127.0.0.1..5x2", "-c", "127.0.0.1..5x2"});

        Properties props = getProperties();

        assertNotNull(props.getProperty("CLIENT_HOSTS"));
        assertEquals(props.getProperty("CLIENT_HOSTS"),
            "127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5,127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void prepareOther() throws Exception {
        PrepareWorker.main(new String[] {"-s", "127.0.0.1..5x2",
            "-scfg", "config/cluster/vmipfinder-remote-server-config.xml",
            "-ccfg", "config/cluster/vmipfinder-remote-client-config.xml",
            "-ibc", "config/cluster/ignite-1000-config.xml",
            "-wd", "work_dir",
            "-b", "2",
            "-jh", "/usr/lib/jvm/java-8-oracle",
            "-fr", "60:120",
            "-jmx",
            "-wp", "work/wal",
            "-wa", "work/wal_arch",
            "-shr", "work/shared",
            "-sn", "work/snapshot"});

        Properties props = getProperties();

        assertNotNull(props);

        assertNotNull(props.getProperty("SERVER_HOSTS"));
        assertEquals(props.getProperty("SERVER_HOSTS"),
            "127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5,127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4,127.0.0.5");

        assertNotNull(props.getProperty("REMOTE_WORK_DIR"));
        assertEquals(props.getProperty("REMOTE_WORK_DIR"),
            "work_dir");

        assertNotNull(props.getProperty("SERVER_CONFIG_FILE"));
        assertEquals(props.getProperty("SERVER_CONFIG_FILE"),
            "work_dir/config/cluster/prepared-vmipfinder-remote-server-config.xml");

        assertNotNull(props.getProperty("CLIENT_CONFIG_FILE"));
        assertEquals(props.getProperty("CLIENT_CONFIG_FILE"),
            "work_dir/config/cluster/prepared-vmipfinder-remote-client-config.xml");

        assertNotNull(props.getProperty("BACKUPS"));
        assertEquals(props.getProperty("BACKUPS"),
            "2");

        assertNotNull(props.getProperty("DEFINED_JAVA_HOME"));
        assertEquals(props.getProperty("DEFINED_JAVA_HOME"),
            "/usr/lib/jvm/java-8-oracle");

        assertNotNull(props.getProperty("JMX_ENABLED"));
        assertEquals(props.getProperty("JMX_ENABLED"),
            "true");

        assertNotNull(props.getProperty("JFR_OPTS"));
        assertEquals(props.getProperty("JFR_OPTS"),
            "60:120");

        assertNotNull(props.getProperty("WAL_PATH"));
        assertEquals(props.getProperty("WAL_PATH"),
            "work/wal");

        assertNotNull(props.getProperty("WAL_ARCHIVE_PATH"));
        assertEquals(props.getProperty("WAL_ARCHIVE_PATH"),
            "work/wal_arch");

        assertNotNull(props.getProperty("SNAPSHOT_PATH"));
        assertEquals(props.getProperty("SNAPSHOT_PATH"),
            "work/snapshot");

        assertNotNull(props.getProperty("SHARED_PATH"));
        assertEquals(props.getProperty("SHARED_PATH"),
            "work/shared");
    }

    /**
     * Clean up.
     */
    @After
    public void cleanUp(){
        File[] cfgs = new File("config/cluster").listFiles();

        for(File f : cfgs)
            if(f.getName().startsWith("prepared"))
                f.delete();

        new File(DEFAULT_PROP).delete();

        new File("prepared.zip").delete();
    }

    private Properties getProperties(){
        Properties prop = new Properties();

        try {
            prop.load(new FileInputStream(DEFAULT_PROP));
        }
        catch (IOException e) {
            printw(String.format("Failed to load property file %s", DEFAULT_PROP));

            return null;
        }

        return prop;
    }

}