# POC Tester

## What is POC Tester?

POC Tester is a tool to prepare, run and get results of distributed tests which simulate some user-alike production
scenario in an Apache Ignite or GridGain cluster.

* Tasks are written in Java and uses public API of Apache Ignite or GridGain to implement the test logic.
* Tasks are configured via Java property files.
* Simple shell scripts are used to prepare, deploy and run a test scenario.
* As an alternative, scripts in Python + YAML files are used to store steps of a test scenario along with scenario 
parameters and to run the scenario.

## Requirements

* GNU/Linux. Tested on Ubuntu 18.04, CentOS 7.
* Oracle JDK 8 or newer, OpenJDK 8 or newer.
* Python 3.5 or newer for AutoPOC scripts.
* SSH access with key-based authentication to hosts used in a test scenario.
* Maven 3.5.x or newer. Tested with Maven 3.5.2.

## How to build

POC Tester uses Maven as a build tool and for dependency management. Uses Apache Ignite + GridGain PE/CE libs of a 
version under test as dependencies. Set Apache Ignite or GridGain version either in [pom.xml](pom.xml) or by passing the 
`-Dignite.version=SOME_VERSION` command-line parameter.

To get a POC Tester build artifact, first one need to `cd` to the directory with local copy of the repository and then 
run Maven to build a POC Tester artifact. E.g.:
```bash
$ cd /home/john_doe/poc-tester
$ mvn clean package -DskipTests -Dignite.version=2.5.10
```

The artifact will be stored in `target/assembly` directory of the local repository.

## Development branches

Main development branches correspond to GG "baselines":
* master -- GG 8.8.x and later
* ignite-2.4-master -- GG 8.4.x
* ignite-2.5-master -- GG 8.5.x
* ignite-2.7-master -- GG 8.7.1, 8.7.2
* gridgain-8.7-master -- GG 8.7.x

## How to run?

There are two ways to run some scenario. The traditional way is to start several shell scripts one by one.
Shell scripts are used to prepare common parameters for test scenario, deploy the prepared POC Tester artifact, 
start server and client nodes. 

Another way is AutoPOC scripts. They are essentially a front-end to the shell scripts. 

### Way 1, traditional and flexible -- plain shell scripts

Shell scripts are located in the [bin](bin) directory.

The shell script below will start a scenario which performs classical task of transferring money between accounts stored
in key-value caches using transactional API of Apache Ignite / GridGain.

```bash
#!/usr/bin/env bash

# This shell script will start a scenario which performs classical task of transferring money between accounts
# stored in key-value caches using transactional API of Apache Ignite / GridGain.

POC_HOME=/home/john_doe/poc-tester
cd ${POC_HOME}/target/assembly

# Prepare the POC Tester artifact and properties file with common parameters
bin/prepare.sh \
    --serverHosts "127.0.0.1" --clientHosts "127.0.0.1" \
    --remoteWorkDir "${POC_HOME}/var" --walPath "${POC_HOME}/var/poc-wal" \
    --walArch "${POC_HOME}/var/poc-wal-arch" \
    --snapshotPath "${POC_HOME}/var/snapshot" \
    --definedJavaHome /usr/lib/jvm/java-8-oracle \
    --backups "2" \
    --importedBaseCfg "config/cluster/caches/caches-base.xml" \
    --serverCfg "config/cluster/inmemory-remote-server-config.xml" \
    --clientCfg "config/cluster/inmemory-remote-client-config.xml" \
    --dataRegionNames "inMemoryConfiguration" \
    --user john_doe \
    --keyPath /home/john_doe/.ssh/id_rsa

# Kill running POC Tester processes on given hosts
bin/kill.sh

# Clean files in the remote work directory
bin/include/clean.sh --cleanAll

# Deploy prepared 
bin/deploy.sh

# Start server nodes on serverHosts
bin/start-servers.sh

# Start clients to preload test data 
bin/start-clients.sh --taskProperties config/load.properties

# Wait for preloading to complete
bin/include/wait-for-lock.sh --lockName loadlock

# Periodically calculate sums of balances across all accounts (the invariant check)
bin/start-clients.sh --taskProperties config/transfer/check-affinity.properties
bin/start-clients.sh --taskProperties config/transfer/tx-balance.properties
bin/start-clients.sh --taskProperties config/restart.properties
```

### Way two, easier and more configurable -- AutoPOC scripts

The AutoPOC scripts are written in Python and uses YAML files to store test scenario steps along with parameters of the test tasks.
It is used to store scenarios in a more readable fashion and to remove the need to manually edit every property file of a task.
AutoPOC's logic is this simple: read a scenario file, prepare `.properties` for test tasks, run shell scripts mentioned in the scenario.
AutoPOC is located in [utils/autopoc](utils/autopoc). YAML files are located in [utils/autopoc/res](utils/autopoc/res).

The same "Transfer" scenario as above will look like [this](utils/auto_poc/res/scenario_transfer.yaml) in AutoPOC's format:

```yaml
parameters:
    - res/params.yaml
    - res/cluster/params_mode_inmem.yaml
setup:
    - prepare
    - kill
    - clean
    - deploy
    - dstat_start
test:
    - start_servers
    - load_task
    - wait_for_lock
    - check_affinity_task
    - tx_balance_task
    - restart_task
teardown:
    - kill
    - dstat_stop
```

Scenario parameters (a fragment of [utils/autopoc/res/params.yaml](utils/autopoc/res/params.yaml))
```yaml
prepare:
  main_parameters:
    script: "bin/prepare.sh"
    remoteWorkDir: /home/john_doe/poc-tester/var
    definedJavaHome: /usr/lib/jvm/java-8-oracle
    user: john_doe
    keyPath: /home/john_doe/.ssh/id_rsa
    walPath: /home/john_doe/poc-tester/var/poc-wal
    walArch: /home/john_doe/poc-tester/var/poc-wal-arch
    snapshotPath: /home/john_doe/poc-tester/var/snapshot
    importedBaseCfg: config/cluster/caches/caches-1000.xml

tx_balance_task:
  override_clientHosts:
    source: clientHosts
    nodes: 3
  main_parameters:
    script: "bin/start-clients.sh"
    taskProperties: config/transfer/tx-balance.properties
  properties_params:
    MAIN_CLASS: TxBalanceTask
    waitForFlags: pauseTX
    # dataRange: *# dataRange
    cacheNamePrefix: cachepoc
    txConcurrency: PESSIMISTIC
    txIsolation: REPEATABLE_READ
    # cacheRange: *# cacheRange
    reportInterval: 10
    # Duration of task
    # timeToWork: *# timeToWork
    threads: 8
    txLen: 2
    # In mixed mode, transfer operations will be done only between PART_A and PART_B caches,
    #  but neither PART_A-to-PART-A nor PART_B-to-PART_B. Use appropriate server and client XML configs!
    #    mixedMode: true
    # Colon-separated ranges of probabilities for different transaction types:
    # 1 - Simple get() and put() transactions.
    # 2 - getAll() and putAll() transactions.
    # 3 - removeAll() and putAll() transactions.
    probRange: "40:30:30"
    # If putGetTxLenProbs=1:1:5:10, then transactions length distribution will be as follows:
    # length=1 will get 50% probability, length=5 - 25%, length=10 - 25%.
    # Colon-separated transactions length probabilities for get() and put() transactions.
    putGetTxLenProbs: "5:5:5:10:10"
    # Colon-separated transactions length probabilities for getAll() and putAll() transactions.
    putAllTxLenProbs: "7:7:12:12"
    # Colon-separated transactions length probabilities for removeAll() and putAll() transactions.
    removeTxProbs: "3:5:7:9"
```

To run the Transfer scenario using AutoPOC:

```bash
#!/usr/bin/env bash

POC_HOME=/home/john_doe/poc-tester
cd ${POC_HOME}/target/assembly/utils/autopoc

python3 ./start.py \
    -server_hosts 127.0.0.1 -client_hosts 127.0.0.1 \ 
    -scenario_yaml res/scenario_transfer.yaml
```

## How to stop?

Most clients have the `timeToWork` property. When the `timeToWork` interval elapses, the task will stop. 
When all tasks finish, AutoPOC will call the `teardown` section where all nodes are stopped. 
There is no such option with traditional scripts.

To stop (kill) remote nodes manually, one can call the [bin/kill.sh](bin/kill.sh) from `target/assembly`.

## Additional helper scripts

All scripts should be started on local node from the `target/assembly` directory.

Log files from remote hosts can be obtained with [bin/collect.sh](bin/collect.sh). Result: `log-XXX` directory is created.

Many tasks collect statistics (throughput, state number, etc). These statistics can be converted to charts.
The script: [bin/include/simple-chart.sh](bin/include/simple-chart.sh). 
Also this script will unpack any gzipped log files.

Results are analysed manually to make a PASS/FAIL verdict. One should search for any errors or exceptions. 
To make a summary of interesting events on cluster nodes, run [bin/include/log-analyzer.sh](bin/include/log-analyzer.sh).
The `log-XXX/analyzed-logs-XXX` directory with summary info from all log files will be created.
To configure which lines to filter, edit [config/log-analyzer-cfg/log-analyzer-context.yaml](config/log-analyzer-cfg/log-analyzer-context.yaml).
