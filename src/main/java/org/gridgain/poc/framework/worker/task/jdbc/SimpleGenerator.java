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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

/**
 * Created by oostanin on 25.01.18.
 */
public class SimpleGenerator implements QueryGenerator {

    private static String[] nameArr = new String[]{"John", "Paul", "Ringo", "George"};
    private static String[] nameArrW = new String[]{"Patty", "Selma", "Marge", "Lisa"};
    private static String[] lastNameArr = new String[]{"Addams", "Buckman", "Bing", "West", "Beck"};
    private static String[] univNameArr = new String[]{"UN1", "UN2", "UN3", "UN4", "UN5"};
    private static String[] stateNameArr = new String[]{"state1", "state2", "state3", "state4", "state5"};
    private static String[] cityNameArr = new String[]{"city1", "city2", "city3", "city4", "city5"};

    public List<String> generate(long num){
        List<String> res = new ArrayList<>();

        String name = "Organisation" + num;

        long emplNum = num * 10;

        String orgQry = String.format("INSERT INTO Organisation(id, name, emplNum) VALUES(%d, '%s', %d)", num, name, emplNum);

        res.add(orgQry);

        Queue<String> names = getNames(nameArr);
        Queue<String> namesW = getNames(nameArrW);
        Queue<String> lastNames = getNames(lastNameArr);
        Queue<String> unvNames = getNames(univNameArr);

        for(int i = 0; i < emplNum; i++ ){
            if (names.size() == 0)
                names = getNames(nameArr);

            if (namesW.size() == 0)
                namesW = getNames(nameArrW);

            if (lastNames.size() == 0)
                lastNames = getNames(lastNameArr);

            if (unvNames.size() == 0)
                unvNames = getNames(univNameArr);

            String firstName;
            String gender;

            Random r = new Random();

            int gen = r.nextInt(2);

            if(gen == 0){
                firstName = names.poll();
                gender = "m";
            }
            else{
                firstName = namesW.poll();
                gender = "f";
            }

            String stateName = stateNameArr[r.nextInt(stateNameArr.length)];
            String cityName = cityNameArr[r.nextInt(cityNameArr.length)];

            String insQry = String.format("INSERT INTO Taxpayers(id, org_id, first_name, last_name, state, city, gender, univ) " +
                    "VALUES(%d, %d, '%s', '%s', '%s', '%s', '%s', '%s')", i, num, firstName, lastNames.poll(),
                stateName, cityName, gender, unvNames.poll());

            res.add(insQry);

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
