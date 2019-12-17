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

package org.apache.ignite.scenario.internal.utils;

import java.util.ArrayList;

/**
 * Created by oostanin on 30.11.17.
 */
public class IpWorker {

    private int mult = 1;

    public static void main(String[] args) {
        if(args.length < 1){
            System.out.println("ERROR! No input string.");
            System.exit(1);
        }

        String input = args[0];

        System.out.println(new IpWorker().convertDots(input));
    }

    public String convertDots(String str) {
        String input = str;

        if (str.contains("x")) {
            String[] mlt = str.split("x");

            if (mlt.length > 2) {
                System.out.println("ERROR! Not a valid input string. You should not use 'x' more then once.");
                System.exit(1);
            }

            input = mlt[0];
            toIntCheck(mlt[1]);
            mult = Integer.valueOf(mlt[1]);
        }

        String[] arr = input.split(",");
        ArrayList<String> list = new ArrayList<>(arr.length);

        for (int i = 0; i < arr.length; i++) {
            list.add(changeDots(arr[i]));
            if (i < arr.length - 1)
                list.add(",");
        }

        ArrayList<String> resList = new ArrayList<>(arr.length);

        for (int i = 0; i < mult; i++) {
            for (String s : list)
                resList.add(s);
            if (i < mult - 1)
                resList.add(",");
        }

        StringBuilder fin = new StringBuilder();

        for (String s : resList)
            fin.append(s);

        return fin.toString();
    }

    private String changeDots(String input) {
        if (!input.contains(".."))
            return input;

        String[] arr = input.split("\\.");

        if(arr.length < 6){
            System.out.println("ERROR! Not a valid input string. You should not use " + input);
            System.exit(1);
        }

        toIntCheck(arr[0]);
        toIntCheck(arr[1]);
        toIntCheck(arr[2]);


        String num1s = arr[3];
        String num2s = arr[5];

        int num1 = toIntCheck(num1s);
        int num2 = toIntCheck(num2s);

        StringBuilder res = new StringBuilder();

        for (int i = num1; i <= num2; i++) {

            res.append(arr[0]);
            res.append(".");

            res.append(arr[1]);
            res.append(".");

            res.append(arr[2]);
            res.append(".");

            res.append(Integer.valueOf(i));

            if (i < num2)
                res.append(',');
        }

        return res.toString();
    }

    private int toIntCheck(String str) {
        int res = 0;

        try {
            res = Integer.valueOf(str);
        }
        catch (NumberFormatException ignored) {
            System.out.println("ERROR! Not a valid input string. You should not use " + str);
            System.exit(1);
        }

        if (res < 0 || res > 255) {
            System.out.println("ERROR! Not a valid input string. You should not use " + str);
            System.exit(1);
        }
        return res;
    }
}
