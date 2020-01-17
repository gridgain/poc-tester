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
import java.io.IOException;
import org.netbeans.lib.profiler.heap.Heap;
import org.netbeans.lib.profiler.heap.HeapFactory;
import org.netbeans.lib.profiler.heap.Instance;

public class HeapDumpChecker {
    public static void main(String[] args) {
        String path = args[0];

        long totalSize = 0;
        long fullSize = 0;

        try {
            Heap heap = HeapFactory.createHeap(new File(path));

            Iterable<Instance> list = heap.getAllInstances();

            for (Instance i : list) {
                if (i.getNearestGCRootPointer() != null)
                    totalSize += i.getSize();

                fullSize += i.getSize();
            }

            System.out.println(String.format("Total_size=%d", totalSize));
            System.out.println(String.format("Full_size=%d", fullSize));
            System.out.println(String.format("File_size=%d", new File(path).length()));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
