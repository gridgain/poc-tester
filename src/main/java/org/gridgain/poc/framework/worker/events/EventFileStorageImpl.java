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

package org.gridgain.poc.framework.worker.events;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;

/**
 * TaskMetricsRecord storage naive implementation.
 */
public class EventFileStorageImpl implements EventStorage, AutoCloseable {
    /** */
    private static final Logger LOG = LogManager.getLogger(EventFileStorageImpl.class.getName());
    /** */
    private final Queue<EventRecord> queue = new ConcurrentLinkedQueue<>();

    /** */
    private final Thread writerThread;

    /** */
    private final WriterWorker writerWorker;

    /** */
    public EventFileStorageImpl(Path baseDir, String taskName) {
        Path dir = baseDir.toAbsolutePath().resolve("eventstore");

        ensureDirExists(dir);

//        String consID = ignite.cluster().localNode().consistentId().toString();
        String consID = System.getProperty("CONSISTENT_ID");

        String other = String.format("%s-events-%s-%s.log", taskName, consID, dateTime());
        Path file = dir.resolve(other);

        assert !Files.exists(file) : file.toAbsolutePath();

        LOG.info("Created file event storage: " + file.toAbsolutePath());

        BufferedWriter writer;
        try {
            writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }

        writerWorker = new WriterWorker(writer, false);

        writerThread = new Thread(null, writerWorker, taskName + "-event-repWriter");

        writerThread.start();
    }

    /** */
    private void ensureDirExists(Path dir) {
        try {
            Files.createDirectories(dir);
        }
        catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException("Can't create directory.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void record(EventRecord evt) {
        assert !writerWorker.stopping : "Event storage repWriter was stopped.";

        queue.offer(evt);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        writerWorker.cancel();

        writerThread.join();
    }

    /** */
    private class WriterWorker implements Runnable {
        /** */
        private final BufferedWriter writer;

        /** */
        private final boolean useRelatedTime;

        /** */
        private long firstEventTimestamp = -1;

        /** */
        volatile boolean stopping;

        /** */
        public WriterWorker(BufferedWriter writer, boolean useRelatedTime) {
            this.writer = writer;

            this.useRelatedTime = useRelatedTime;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            LOG.debug("TaskMetricsRecord storage repWriter started.");
            TaskMetricsRecord evt;

            try {
                writer.write("Timestamp (ms),Duration (mcs)");
                writer.newLine();

                while (true) {
                    try {
                        synchronized (this) {
                            while ((evt = (TaskMetricsRecord)queue.poll()) == null && !stopping)
                                wait(500);

                        }
                    }
                    catch (InterruptedException e) {
                        stopping = true;

                        continue;
                    }

                    if (evt == null && stopping)
                        break;

                    writeEvent(evt);
                }

                writer.flush();
            }
            catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }
            finally {
                U.closeQuiet(writer);

                LOG.debug("TaskMetricsRecord storage repWriter has been stopped.");
            }
        }

        /** */
        private void cancel() {
            synchronized (this) {
                stopping = true;

                notifyAll();
            }
        }

        /** */
        private long writeEvent(TaskMetricsRecord evt) throws IOException {
            if (useRelatedTime && firstEventTimestamp < 0) {
                firstEventTimestamp = evt.getTimestamp();

                writer.write(String.valueOf(firstEventTimestamp));
            }
            else
                writer.write(String.valueOf(evt.getTimestamp() - firstEventTimestamp));

            writer.write(',');
            writer.write(String.valueOf(TimeUnit.NANOSECONDS.toMicros(evt.getDuration())));
            writer.newLine();

            return firstEventTimestamp;
        }

    }
}
