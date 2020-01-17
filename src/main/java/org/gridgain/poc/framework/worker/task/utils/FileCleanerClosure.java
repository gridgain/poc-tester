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

import java.io.File;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Checks heap size of JVM in which Ignite node runs.
 */
public class FileCleanerClosure implements IgniteClosure<String, Boolean> {
    /** */
    private static final Logger LOG = LogManager.getLogger(FileCleanerClosure.class.getName());

    /** {@inheritDoc} */
    @Override public Boolean apply(String dumpPath) {
        File dumpFile = new File(dumpPath);

        if (dumpFile.exists()) {
            LOG.info(String.format("Deleting file %s", dumpPath));
            return dumpFile.delete();
        }

        return true;
    }
}
