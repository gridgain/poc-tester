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

package org.gridgain.poc.framework.model.processing;

public class AcquiringTx {
    private long SRCPAN;

    private long TGTACCT;

    private long SRCACCT;

    private long AMOUNT;

    private Boolean RECONCILED;

    private Boolean REPLICATED;

    public AcquiringTx(long SRCPAN, long TGTACCT, long SRCACCT, long AMOUNT) {
        this.SRCPAN = SRCPAN;
        this.TGTACCT = TGTACCT;
        this.SRCACCT = SRCACCT;
        this.AMOUNT = AMOUNT;
    }

    public void setReconciled(Boolean reconciled) {
        this.RECONCILED = reconciled;
    }

    public void setREPLICATED(Boolean REPLICATED) {
        this.REPLICATED = REPLICATED;
    }
}
