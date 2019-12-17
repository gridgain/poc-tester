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

package org.apache.ignite.scenario.internal.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.scenario.internal.Utils;

/**
 * Created by oostanin on 19.10.17.
 */
public class SampleObject implements Serializable {
    @QuerySqlField
    private int salary;

    private long key;

    private int balance;

    private Map<String, String> fields;

    public SampleObject(long key, int fieldNum, int fieldSize){
        this.key = key;

        createSalary();
        createFieldsMap(fieldNum, fieldSize);
    }

    private SampleObject(long key, int salary, int balance, Map<String, String> fields) {
        this.key = key;
        this.salary = salary;
        this.balance = balance;
        this.fields = fields;
    }

    public SampleObject copy() {
        return new SampleObject(key, salary, balance, fields);
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public int getBalance() {
        return balance;
    }

    public void setBalance(int balance) {
        this.balance = balance;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    private void createFieldsMap(int fieldNum, int fieldSize) {
        fields = new HashMap<>();

        for (int i = 0; i < fieldNum; i++)
            fields.put("field" + i, Utils.randomString(fieldSize));

    }

    private void createSalary(){
        int random = new Random().nextInt(100);

        if (random < 5)
            salary = 100000;

        if (random >= 5 && random < 15)
            salary = 10000;

        if (random >= 15 && random < 35)
            salary = 5000;

        if (random >= 35)
            salary = 1000;

//        balance = salary * 240;
        balance = 100_000;
    }

//    @Override public int hashCode() {
//        return fields.hashCode() + salary + balance;
//    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SampleObject that = (SampleObject) o;
        return salary == that.salary &&
                key == that.key &&
                balance == that.balance &&
                fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(salary, key, balance, fields);
    }
}
