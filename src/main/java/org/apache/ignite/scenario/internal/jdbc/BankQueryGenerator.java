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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

/**
 * Created by oostanin on 25.01.18.
 */
public class BankQueryGenerator  implements QueryGenerator{

    public List<String> generate(long num){
        List<String> res = new ArrayList<>();

        long bankId = num;

        long countryCode = bankId / 10;

        long firstCardHolderId = bankId * 1000L;

        long lastCardHolderId = firstCardHolderId + 1000L;

        String orgQry = String.format("INSERT INTO Banks(id, country_code) VALUES(%d, %d)", bankId, countryCode);

        res.add(orgQry);

        for(long i = firstCardHolderId ; i < lastCardHolderId; i++ ){
            String cardHolderBanksInsert = String.format("INSERT INTO Cardholder_Banks(cardholder_bank_id, bank_id, cardholder_id) VALUES(%d, %d, %d)", i, bankId, i);
            String cardHolderCadrsInsert = String.format("INSERT INTO Cardholder_Cards(card_number, cardholder_id, card_type_code, currency_code) VALUES(%d, %d, %d, %d)", i, i, 0 ,0);
            String amountsInsert = String.format("INSERT INTO Accounts(id, cardholder_id, amount) VALUES(%d, %d, %d)", i, i, 100_000);


            res.add(cardHolderBanksInsert);
            res.add(cardHolderCadrsInsert);
            res.add(amountsInsert);
        }



        return res;
    }



    private Queue<String> getNames(String[] arr){
        Queue<String> res = new LinkedList();

        for(String name : arr)
            res.add(name);

        return res;
    }
}
