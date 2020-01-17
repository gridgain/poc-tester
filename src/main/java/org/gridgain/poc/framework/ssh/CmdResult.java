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

package org.gridgain.poc.framework.ssh;

import java.util.ArrayList;
import java.util.List;

/**
 * Command execution result.
 */
public class CmdResult {
    /** */
    private int exitCode;

    /** */
    private List<String> outList;

    /** */
    private List<String> errList;

    /** */
    private Process proc;

    /**
     * Constructor.
     *
     * @param exitCode Exit code.
     * @param outList Output stream.
     * @param errList Error stream.
     * @param proc Process.
     */
    public CmdResult(int exitCode, List<String> outList, List<String> errList, Process proc) {
        this.exitCode = exitCode;
        this.outList = new ArrayList<>(outList);
        this.errList = new ArrayList<>(errList);
        this.proc = proc;
    }

    /**
     *
     * @return Command execution result with exit code 1 and empty output and error stream.
     */
    public static CmdResult emptyFailedResult(){
        return new CmdResult(1, new ArrayList<>(), new ArrayList<>(), null);
    }

    /**
     *
     * @return Exit code.
     */
    public int exitCode() {
        return exitCode;
    }

    /**
     *
     * @param exitCode New exit code.
     */
    public void exitCode(int exitCode){
        this.exitCode = exitCode;
    }

    /**
     *
     * @return Output stream.
     */
    public List<String> outputList() {
        return new ArrayList<>(outList);
    }

    /**
     *
     * @return Error stream.
     */
    public List<String> errorList() {
        return new ArrayList<>(errList);
    }

    /**
     *
     * @return Process.
     */
    public Process process() {
        return proc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CommandExecutionResult{" +
            "exitCode=" + exitCode +
            ", outputList=" + outList +
            ", errorList=" + errList +
            '}';
    }
}
