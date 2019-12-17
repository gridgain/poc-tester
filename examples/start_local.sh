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

cd ${POC_HOME}/target/assembly

REMOTE_WORK_DIR=${POC_HOME}/var
WAL_PATH=${POC_HOME}/var/poc-wal
WAL_ARCHIVE_PATH=${POC_HOME}/var/poc-wal-arch
SNAPSHOT_PATH=${POC_HOME}/var/snapshot
SUFFIX=DATETIME

SRV_CFG="config/cluster/inmemory-remote-server-config.xml"
CLIENT_CFG="config/cluster/inmemory-remote-client-config.xml"

#CACHE_CFG="config/cluster/caches/caches-1000.xml"
CACHE_CFG="config/cluster/caches/caches-base.xml"

#SERVERS="127.0.0.1,127.0.0.1"
SERVERS="127.0.0.1"

CLIENTS="127.0.0.1"

JDK_VER=8

SERVER1=$(echo $SERVERS | cut -f 1 -d ',')

bin/prepare.sh \
    --serverHosts ${SERVERS} \
    --clientHosts ${CLIENTS} \
    --remoteWorkDir ${REMOTE_WORK_DIR} \
    --walPath ${WAL_PATH} \
    --walArch ${WAL_ARCHIVE_PATH} \
    --definedJavaHome /usr/lib/jvm/java-${JDK_VER}-oracle \
    --backups "1" \
    --importedBaseCfg $CACHE_CFG \
    --serverCfg ${SRV_CFG} \
    --clientCfg ${CLIENT_CFG} \
    --dataRegionNames "inMemoryConfiguration" \
    --user gridgain \
    --keyPath /home/gridgain/.ssh/id_rsa


bin/kill.sh
bin/include/clean.sh --cleanAll
bin/deploy.sh

#bin/include/stats.sh --dstat start --sudo

bin/start-servers.sh

bin/start-clients.sh --taskProperties config/load.properties
bin/include/wait-for-lock.sh --lockName loadlock
bin/start-clients.sh --taskProperties config/transfer/check-affinity.properties

bin/start-clients.sh --taskProperties config/transfer/tx-balance.properties
