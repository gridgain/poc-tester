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

package org.apache.ignite.scenario.internal.jdbc;

import java.util.ArrayList;
import java.util.List;

public class CardholdersGenerator implements QueryGenerator {
    private static final String CARDHOLDERS_TABLE_NAME = "Cardholders";

    private static final String ACCOUNTS_TABLE_NAME = "Accounts";

    private static final String CARDHOLDERS_INSERT_QRY = "INSERT INTO " + CARDHOLDERS_TABLE_NAME +
            " (pan, firstName, lastName) VALUES (%d, '%s', '%s')";

    private static final String ACCOUNTS_INSERT_QRY = "INSERT INTO " + ACCOUNTS_TABLE_NAME +
            " (acctId, pan, balance) VALUES (%d, %d, %d)";

    private static final long BALANCE_INITIAL = 1_000_000;

    public static final long CARD_TO_ACCOUNT_MULTIPLIER = 16;

    @Override
    public List<String> generate(long num) {
        List<String> res = new ArrayList<>();

        // TODO: generate random names
        String first_name = "Mr.";
        String last_name = "Cardholder";

        long account = num * CARD_TO_ACCOUNT_MULTIPLIER;
        long balance = BALANCE_INITIAL;

        res.add(String.format(CARDHOLDERS_INSERT_QRY, num, first_name, last_name));

        res.add(String.format(ACCOUNTS_INSERT_QRY, account, num, balance));

        return res;
    }
}
