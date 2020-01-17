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

import org.apache.ignite.lang.IgniteClosure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileCompressorClosure implements IgniteClosure<String, Boolean> {
    /** */
    private static final Logger LOG = LogManager.getLogger(FileCompressorClosure.class.getName());

    /** {@inheritDoc} */
    @Override public Boolean apply(String dumpPath) {
        File dumpFile = new File(dumpPath);
        String zipPath = dumpPath + ".zip";

        if (dumpFile.exists()) {
            LOG.info("Compressing file {} to {}", dumpPath, zipPath);

            try {
                FileOutputStream fos = new FileOutputStream(zipPath);
                ZipOutputStream zipOut = new ZipOutputStream(fos);
                FileInputStream fis = new FileInputStream(dumpFile);
                ZipEntry zipEntry = new ZipEntry(dumpFile.getName());

                zipOut.putNextEntry(zipEntry);

                byte[] buffer = new byte[1024];
                int length;

                while ((length = fis.read(buffer)) >= 0) {
                    zipOut.write(buffer, 0, length);
                }

                zipOut.close();
                fis.close();
                fos.close();

                LOG.info(String.format("Deleting file %s", dumpPath));
                return dumpFile.delete();
            }
            catch (IOException e) {
                LOG.error("Failed to compress {} to {}", dumpPath, zipPath);
                LOG.error(e);
                return false;
            }
        }

        return true;
    }

}
