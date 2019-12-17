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

POC_HOME=`cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd`

cd $POC_HOME/utils/auto_poc

ADD_PARAMS=""
#ADD_PARAMS="-params_yaml res/params_jdk11.yaml"

SRV_HOSTS="127.0.0.1,127.0.0.1"
CLIENT_HOSTS="127.0.0.1"

python3 -u ./start.py $ADD_PARAMS -server_hosts $SRV_HOSTS -client_hosts $CLIENT_HOSTS \
    -scenario_yaml res/scenario_smoke.yaml
