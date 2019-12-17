#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Define script directory.
SCRIPT_HOME=$(cd $(dirname "$0"); pwd)
POC_TESTER_HOME=$(dirname "${SCRIPT_HOME}")

if [[ ${POC_TESTER_HOME} == *bin ]];then
    POC_TESTER_HOME=$(dirname "${POC_TESTER_HOME}")
fi

MAIN_TIME=$(date +"%Y-%m-%d-%H-%M-%S")

CP="${CP}:${POC_TESTER_HOME}/libs/*:${POC_TESTER_HOME}/poc-tester-libs/*"

#
# Discovers path to Java executable and checks it's version.
# The function exports JAVA variable with path to Java executable.
#
checkJava() {
    # Check JAVA_HOME.
    if [ "$JAVA_HOME" = "" ]; then
        JAVA=`type -p java`
        RETCODE=$?

        if [ $RETCODE -ne 0 ]; then
            echo $0", ERROR:"
            echo "JAVA_HOME environment variable is not found."
            echo "Please point JAVA_HOME variable to location of JDK 1.8 or JDK 9."
            echo "You can also download latest JDK at http://java.com/download"

            exit 1
        fi

        JAVA_HOME=
    else
        JAVA=${JAVA_HOME}/bin/java
    fi

    #
    # Check JDK.
    #
   "$JAVA" -version 2>&1 | grep -qE 'version "(1.8.*|9.*)"' || {
        echo "$0, ERROR:"
        echo "The version of JAVA installed in JAVA_HOME=$JAVA_HOME is incorrect."
        echo "Please point JAVA_HOME variable to installation of JDK 1.8 or JDK 9."
        echo "You can also download latest JDK at http://java.com/download"
        exit 1
    }
}

checkJava
