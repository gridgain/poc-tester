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

package org.apache.scenario.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.ignite.scenario.internal.utils.IgniteNode;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;

/** */
public class NodeRunTest {
    /** */
    private String tmpServCfg;

    /** */
    Ignite ignite;

    /**
     *
     * @throws IOException
     */
    @Before
    public void editCfgForTest() throws IOException {
        String pathOrig = "config/cluster/vmipfinder-remote-server-config.xml";
        tmpServCfg = String.format("config/cluster/tmp-serv-cfg-%s.xml", dateTime());

        editConfig(pathOrig, tmpServCfg);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void startNode() throws Exception {
        PocTesterArguments args = new PocTesterArguments();

        args.setServerCfg(tmpServCfg);

        ignite = new IgniteNode(false).start(args);
    }

    @After
    public void cleanUp() {
        new File(tmpServCfg).delete();

        new File("${env:LOG_FILE_NAME}").delete();

        ignite.close();
    }

    public void editConfig(String pathOrig, String pathRes) throws IOException {
        List<String> resLines = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(pathOrig))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("<!--to_be_replaced_by_IP_list-->"))
                    line = "                                <value>127.0.0.1:47500..47509</value>";

                if (line.contains("${CONSISTENT_ID}"))
                    line = String.format("        <property name=\"consistentId\" value=\"CONSISTENT_ID\"/>");

                resLines.add(line);
            }
        }

        PocTesterUtils.updateFile(Paths.get(pathRes), resLines);
    }

}
