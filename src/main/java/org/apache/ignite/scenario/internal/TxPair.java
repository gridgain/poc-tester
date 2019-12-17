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

package org.apache.ignite.scenario.internal;

import org.jetbrains.annotations.NotNull;

public class TxPair implements Comparable<TxPair> {
    private Long key0;
    private Long key1;

    private String cacheName0;
    private String cacheName1;

    public TxPair(Long key0, Long key1, String cacheName0, String cacheName1) {
        this.key0 = key0;
        this.key1 = key1;
        this.cacheName0 = cacheName0;
        this.cacheName1 = cacheName1;
    }

    @Override public int compareTo(@NotNull TxPair o) {
        return this.key0.compareTo(o.getKey0());
    }

    public Long getKey0() {
        return key0;
    }

    public Long getKey1() {
        return key1;
    }

    public String getCacheName0() {
        return cacheName0;
    }

    public String getCacheName1() {
        return cacheName1;
    }

    @Override public String toString() {
        return String.format("[TxPair: [key0 = %d; cacheName0 = %s]; [key1 = %d; cacheName1 = %s]]",
            key0, cacheName0, key1, cacheName1);
    }
}
