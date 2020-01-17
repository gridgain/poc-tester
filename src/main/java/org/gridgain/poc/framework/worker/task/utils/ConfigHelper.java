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

import org.gridgain.poc.framework.utils.PocTesterUtils;

import java.io.PrintWriter;
import java.util.List;

/**
 * Created by oostanin on 08.02.18.
 */
public class ConfigHelper {

    public static void main(String[] args) {
        List<String> r = PocTesterUtils.combine5();

        try {
            PrintWriter out = new PrintWriter("caches.txt", "UTF-8");

            for (String s : r) {
                out.println(s);
            }
            out.println("\n");

            out.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
