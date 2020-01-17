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

package org.gridgain.poc.framework.worker.task;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * Parent for reporting tasks.
 */
public abstract class AbstractReportingTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(AbstractReportingTask.class.getName());

    /** Report writer. */
    private BufferedWriter repWriter;

    /**
     * Constructor.
     *
     * @param args Arguments.
     */
    public AbstractReportingTask(PocTesterArguments args) {
        super(args);
    }

    /**
     * @param args Arguments.
     * @param props Task properties.
     */
    public AbstractReportingTask(PocTesterArguments args, TaskProperties props) {
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        createReportFile();
    }

    /**
     * Creates report file.
     */
    private void createReportFile() {
        Path reportDir = Paths.get(homeDir, "log", clientDirName, "reports");

        String consID = System.getProperty("CONSISTENT_ID");

        Path report = Paths.get(homeDir, "log", clientDirName, "reports", String.format("report-%s.log", consID));

        try {
            if (!reportDir.toFile().exists())
                Files.createDirectories(reportDir);

            repWriter = Files.newBufferedWriter(report, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }

        catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public String getTaskReport() throws Exception {
        long timeStamp = System.currentTimeMillis() / 1000L * 1000L;

        String repStr = reportString();

        if (repStr != null) {
            try {
                repWriter.write(String.format("%d,%s", timeStamp, repStr));
                repWriter.newLine();
                repWriter.flush();
            }
            catch (IOException e) {
                LOG.error("Failed to write report: " + e.getMessage());

                e.printStackTrace();
            }
        }

        //TODO: avoid null result.
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void addPropsToMap(TaskProperties props) {
        super.addPropsToMap(props);

        propMap.put("reportDir", "reports");
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        synchronized (AbstractReportingTask.class) {
            try {
                if (repWriter != null)
                    repWriter.close();
            }
            catch (IOException e) {
                LOG.error("Failed to close report writer!", e);
            }
        }

        super.tearDown();
    }

    /**
     * Get report string from task.
     *
     * @return {@code String} report.
     */
    protected abstract String reportString() throws Exception;

}
