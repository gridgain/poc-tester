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

package org.gridgain.poc.framework.worker.task.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by oostanin on 25.01.18.
 */
public class BalanceGenerator implements QueryGenerator {

    public List<String> generate(long num){
        List<String> res = new ArrayList<>();

        long balance = 10_000L;

        String orgQry = String.format("INSERT INTO Balance(id, balance) VALUES(%d, %d)", num, balance);

        res.add(orgQry);

        return res;
    }
}
