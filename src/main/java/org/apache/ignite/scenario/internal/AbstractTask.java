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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TimeStampRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.scenario.GridTask;
import org.apache.ignite.scenario.internal.events.EventFileStorageImpl;
import org.apache.ignite.scenario.internal.events.EventRecord;
import org.apache.ignite.scenario.internal.events.EventStorage;
import org.apache.ignite.scenario.internal.events.TaskMetricsRecord;
import org.apache.ignite.scenario.internal.utils.AbstractWorker;
import org.apache.ignite.scenario.internal.utils.IgniteNode;
import org.apache.ignite.scenario.internal.utils.PocTesterUtils;
import org.apache.ignite.scenario.internal.utils.SSHCmdWorker;
import org.apache.ignite.scenario.internal.utils.StatWorker;
import org.apache.ignite.scenario.internal.utils.NodeInfo;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.CacheException;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.dateTime;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;
import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.sleep;

/**
 * Abstract task class.
 */
public abstract class AbstractTask extends AbstractWorker implements GridTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(AbstractTask.class.getName());

    /** */
    protected static final Character SEP = File.separatorChar;

    /** */
    private static final String DEFAULT_CACHE_NAME = "default";

    /** Cache for storing flags used to synchronize tasks execution. */
    public static final String MSG_CACHE_NAME = "msgCache";

    /** */
    private static final String DEFAULT_CACHE_NAME_PREFIX = "cachepoc";

    /** */
    private static final String DEFAULT_DATA_RANGE = "0:999";

    /** */
    private static final String DEFAULT_CACHE_RANGE = "0:19";

    /** (in seconds) */
    protected static final long DEFAULT_INTERVAL = 1L;

    /** */
    protected static final int DEFAULT_TX_LEN = 1;

    /** */
    static final int DFLT_MIN_TX_LEN = 1;

    /** */
    private static final int DFLT_SERVICE_NUM = 500;

    protected static final boolean DFLT_CACHE_PAIR_SAME_BACKUPS = false;

    /** If true, then caches within a cache pair will be chosen to have equal number of backups. */
    protected boolean cachePairSameBackups;

    /** (in millis) */
    protected long interval;

    /** Lock file name. */
    protected String lockFile;

    /** */
    protected Map<String, Object> propMap;

    /** */
    protected TaskProperties props;

    /** */
    protected String homeDir;

    /** */
    protected String clientDirName;

    /** */
    protected String taskName;

    /** */
    protected String cacheName = DEFAULT_CACHE_NAME;

    /** */
    protected String cacheNamePrefix = DEFAULT_CACHE_NAME_PREFIX;

    /** */
    private Long waitBeforeStart;

    /** */
    protected String cacheRange;

    /** */
    protected int cacheRangeStart;

    /** */
    protected int cacheRangeEnd;

    private String txLen = "1";

    /** */
    protected int txLenNum = DEFAULT_TX_LEN;

    /** */
    protected int minTxLen = DFLT_MIN_TX_LEN;

    /** */
    private static final int DFLT_FIELDS_COUNT = 32;

    /** */
    private static final int DFLT_FIELD_LENGTH = 32;

    /** */
    protected TxLenCalcMode txLenCalcMode = TxLenCalcMode.PREDEFINED;

    /** */
    private List<String> cacheNameList;

    /** Task execution time. (in secs) */
    protected long timeToWork;

    /** Run tasks copies in given number of threads. */
    protected int threads;

    /** */
    private boolean metricsEnabled;

    /** */
    private boolean activateCluster;

    /** */
    private boolean waitForExpSize;

    /** */
    protected boolean mixedMode;

    protected boolean uniqKeysForThread;

    /** Metrics event storage. */
    private EventStorage evtStor;

    /** Key range. */
    protected String dataRange;

    /** Start point for full load. */
    protected long dataRangeFrom;

    /** Finish point for full load. */
    protected long dataRangeTo;

    /** Start point for this client load. */
    protected long taskLoadFrom;

    /** Finish point for this client load. */
    protected long taskLoadTo;

    /** Fields per entry. */
    protected int fieldCnt = DFLT_FIELDS_COUNT;

    /** Field content size. */
    protected int fieldLen = DFLT_FIELD_LENGTH;

    /** Batch size */
    protected int batchSize;

    /** */
    protected int totalNodesNum;

    /** */
    protected int nodeCntr;

    /** */
    private static boolean stopped;

    /**
     * Task will print intermediate results every {@link #reportInterval} (in millis) if it is supported and {@link
     * #reportInterval} > 0.
     *
     * Every task should bother about it in it's body method.
     */
    protected long reportInterval;

    /** A task will pause execution if at least one flag from the list is cleared. */
    protected String[] waitForFlags;

    /** A task will clear the flag(s) before the start of every execution cycle and raise it at the end of the cycle. */
    protected String[] syncFlags;

    /** A task will stop if at least one flag from the list is set */
    protected String[] stopFlags;

    public static final String DEFAULT_STOP_FLAG = "dataLost";

    /** Client node connected to grid */
    private Ignite ignite;

//    /** */
//    private static AtomicBoolean nodeStarted = new AtomicBoolean(false);
//
//    private static AtomicBoolean eventStorageCreated = new AtomicBoolean(false);

    /** */
    private static long lastThreadDumpTime;

    /** */
    private long expTotalSize;

    /** */
    private long taskStartTime;

    protected AtomicInteger totalIterCnt = new AtomicInteger();

    protected List<String> cacheNamesA;

    protected List<String> cacheNamesB;

    /** */
    protected IgniteServices svcs;

    /** Number of service to deploy */
    private int serviceNum;

    /** */
    public AbstractTask() {
    }

    /** */
    public AbstractTask(PocTesterArguments args) {
        this.args = args;
    }

    /** */
    public AbstractTask(PocTesterArguments args, TaskProperties props) {
        this.args = args;
        this.props = props;
    }

    /**
     * Init task parameters from given properties.
     *
     * NOTE: All child classes should call super method if override.
     */
    @Override public void init() throws IOException {
        setDirs();

        totalNodesNum = args.getTotalNodesNum();

        nodeCntr = args.getNodeCntr();

        propMap = new HashMap<>();

        Map<String, String> hdrMap = new HashMap<>();

        hdrMap.put("time", "milliseconds");

        propMap.put("headersMap", hdrMap);

        addPropsToMap(props);

        taskName = getClass().getSimpleName();

        threads = props.getInteger(TaskProperties.TASK_THREADS_PROPERTY, 1);

        // Interval must be defined in seconds in property file.
        interval = props.getLong("interval", DEFAULT_INTERVAL) * 1000L;

        reportInterval = props.getLong(TaskProperties.PARAMETER_REPORT_INTERVAL, 60) * 1000L;

        timeToWork = props.getLong(TaskProperties.PARAMETER_TIME_TO_WORK, -1);

        cacheName = props.getString("cacheName", DEFAULT_CACHE_NAME);

        cacheNamePrefix = props.getString("cacheNamePrefix", DEFAULT_CACHE_NAME_PREFIX);

        waitBeforeStart = props.getLong("waitBeforeStart", 0L);

        cacheRange = props.getString("cacheRange", DEFAULT_CACHE_RANGE);
        cacheRangeStart = parseRangeInt(cacheRange)[0];
        cacheRangeEnd = parseRangeInt(cacheRange)[1];

        fieldCnt = props.getInteger("fieldCount", DFLT_FIELDS_COUNT);

        fieldLen = props.getInteger("fieldLength", DFLT_FIELD_LENGTH);

        batchSize = props.getInteger("batchSize", 1000);

        lockFile = props.getString("lockFile");

        metricsEnabled = props.getBoolean("metricsEnabled", Boolean.TRUE);

        activateCluster = props.getBoolean("activateCluster", Boolean.TRUE);

        waitForExpSize = props.getBoolean("waitForExpSize", Boolean.FALSE);

        mixedMode = props.getBoolean("mixedMode", Boolean.FALSE);

        uniqKeysForThread = props.getBoolean("uniqKeysForThread", Boolean.FALSE);

        dataRange = props.getString("dataRange", DEFAULT_DATA_RANGE);
        dataRangeFrom = parseRangeLong(dataRange)[0];
        dataRangeTo = parseRangeLong(dataRange)[1];
        LOG.info(String.format("Data range is set to dataRangeFrom = %d; dataRangeTo = %d", dataRangeFrom, dataRangeTo));

        expTotalSize = props.getLong("expTotalSize", 0L);

        txLen = props.getString("txLen", "1");
        setTxLenRange();
        LOG.info(String.format("Tx length range is set to minTxLen = %d; txLenNum = %d", minTxLen, txLenNum));

        serviceNum = props.getInteger("serviceNum", DFLT_SERVICE_NUM);

        if (props.getString("waitForFlags") != null)
            waitForFlags = props.getString("waitForFlags").split(",");

        if (props.getString("syncFlags") != null)
            syncFlags = props.getString("syncFlags").split(",");

        stopFlags = props.getString("stopFlags", DEFAULT_STOP_FLAG).split(",");

        cachePairSameBackups = props.getBoolean("cachePairSameBackups", DFLT_CACHE_PAIR_SAME_BACKUPS);
    }

    /**
     * Runs before body starting.
     *
     * Start client node.
     *
     * NOTE: All child classes should call super method if override.
     */
    @Override public void setUp() throws Exception {
        if (isMetricsEnabled())
            evtStor = new EventFileStorageImpl(Paths.get(homeDir, "log", clientDirName), taskName);

        if (!disableClient()) {
            startNode();

            cacheNameList = setCacheNameList();
        }
        else
            println("Task started without launching ignite node.");

        if (lockFile != null)
            createLockFile();

        exportConfig();

        taskStartTime = System.currentTimeMillis();
    }

    /**
     * Starts Ignite node.
     */
    private void startNode() {
        String cfgPath = args.getClientCfg();

        try {
            ignite = new IgniteNode(true).start(args);
        }
        catch (Exception e) {
            LOG.error("Failed to start node.", e);
        }

        if (waitBeforeStart != null) {
            LOG.info(String.format("Waiting %d seconds before start.", waitBeforeStart));

            sleep(waitBeforeStart * 1000L);
        }

        if (!ignite().cluster().active() && activateCluster) {
            LOG.info("Activating cluster.");

            ignite().cluster().active(true);

            sleep(2000L);
        }

        LOG.info("ignite().cluster().active() has returned: " + ignite().cluster().active());
    }

    /** */
    @Override public void body() throws Exception {
        final long taskStartTime = System.currentTimeMillis();

        final long iterStartTime = System.nanoTime();

        body0();

        if (metricsEnabled)
            recordEvent(new TaskMetricsRecord(taskStartTime, System.nanoTime() - iterStartTime));

        totalIterCnt.getAndIncrement();
    }

    /** Get task state for long running tasks. */
    @Nullable @Override public String getTaskReport() throws Exception {
        //TODO: avoid null result.
        return null;
    }

    /** Task body. */
    protected abstract void body0() throws Exception;

    /**
     * Runs after body has finished.
     *
     * Stops client node.
     *
     * NOTE: All child classes should call super method if override.
     */
    @Override public void tearDown() {
        if (lockFile != null)
            new File(homeDir + SEP + lockFile).delete();

        if (evtStor != null)
            U.closeQuiet(evtStor);

        if (ignite != null)
            closeIgnite();
    }

    /**
     * @return instance of Ignite client node.
     */
    public Ignite ignite() {
        return ignite;
    }

    /**
     * @param ignite IgniteNode.
     */
    public void ignite(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * @return {@code True} if task metrics is enabled.
     */
    private boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /** */
    private void setDirs() {
        homeDir = System.getProperty("pocTesterHome");
        clientDirName = System.getProperty("clientDirName");
    }

    /**
     * Disables client node. {@code False} by default.
     *
     * @return {@code false} if client node should be started, {@code true} otherwise.
     */
    protected boolean disableClient() {
        return false;
    }

    /** */
    private void closeIgnite() {
        if (waitForExpSize && expTotalSize != 0) {
            while (expTotalSize < getTotalCachesSize()) {
                LOG.info("Total cache size is less than expected. Waiting 10 seconds.");

                sleep(10_000L);
            }
        }

        ignite.close();
    }

    /**
     * Record event.
     *
     * @param evt event.
     */
    private void recordEvent(EventRecord evt) {
        if (evtStor != null)
            evtStor.record(evt);
    }

    /** Export effective task configuration to file. */
    private void exportConfig() {
        String effectiveCfgFile = Paths.get(homeDir, "log", clientDirName, "task-properties.json").toString();

        try {
            PocTesterUtils.saveJson(effectiveCfgFile, propMap);
        }
        catch (IOException e) {
            LOG.error("Failed to export task config to file: " + effectiveCfgFile);

            new Exception("Failed to export task config to file: " + effectiveCfgFile, e).printStackTrace(System.out);
        }
    }

    /**
     * Check cache mane if it is in defined range.
     *
     * @param cacheName Cache name to check.
     * @return boolean flag.
     */
    protected boolean checkNameRange(String cacheName) {
        String prefix = cacheNamePrefix;

        // Extract the number at the end of cacheName
        Pattern p = Pattern.compile(prefix + ".*?([0-9]+)$");
        Matcher m = p.matcher(cacheName);

        if (m.matches()) {
            int suffix = Integer.valueOf(m.group(1));
            if (suffix >= cacheRangeStart && suffix <= cacheRangeEnd)
                return true;
        }

        return false;
    }

    protected long getRandomKey() {
        return dataRangeFrom + ThreadLocalRandom.current().nextLong(dataRangeTo - dataRangeFrom + 1);
    }

    /**
     *
     */
//    protected Map<Long, String> getUniqKeys() {
//        int calcTxLen = txLenCalcMode == TxLenCalcMode.PREDEFINED ? txLenNum :
//            minTxLen + new Random().nextInt(txLenNum - minTxLen);
//
//        Map<Long, String> res = new HashMap<>(calcTxLen * 2);
//
//        for (int i = 0; i < calcTxLen * 2; i++) {
//            long key = getRandomKey();
//
//            while (res.keySet().contains(key))
//                key = getRandomKey();
//
//            Random random = new Random();
//
//            int pos = random.nextInt(getCacheNameList().size());
//
//            String cacheName = getCacheNameList().get(pos);
//
//            res.put(key, cacheName);
//        }
//
//        return res;
//    }

    /**
     * TODO: to be refactored.
     */
//    protected void waitForLoad(Logger log) {
//        boolean waitForLoad = true;
//
//        while (waitForLoad) {
//            waitForLoad = false;
//
//            for (String cacheName : getCacheNameList()) {
//                if (ignite().cache(cacheName).get(keyRange - 1) == null) {
//
//                    waitForLoad = true;
//
//                    long pause = 5000;
//
//                    log.info("Key " + (keyRange - 1) + " is not loaded in cache " + cacheName + ". Waiting " +
//                        (pause / 1000L) + " seconds.");
//
//                    try {
//                        new CountDownLatch(1).await(pause, TimeUnit.MILLISECONDS);
//                    }
//                    catch (InterruptedException e) {
//                        LOG.error("Failed to wait for load", e);
//                    }
//                }
//            }
//        }
//    }

    /**
     *
     */
    protected void waitForAffinity() {
        boolean completed = false;

        int maxTry = 100;

        long timeout = 60_000L;

        while (!completed && maxTry-- > 0) {
            AffinityTopologyVersion waitTopVer = ((IgniteEx)ignite()).context().discovery().topologyVersionEx();

            if (waitTopVer.topologyVersion() <= 0)
                waitTopVer = new AffinityTopologyVersion(1, 0);

            IgniteInternalFuture<?> exchFut =
                ((IgniteEx)ignite()).context().cache().context().exchange().affinityReadyFuture(waitTopVer);

            if (exchFut != null) {
                try {
                    exchFut.get(timeout);

                    completed = exchFut.isDone();
                }
                catch (IgniteCheckedException ignored) {
                    LOG.warn(String.format("Failed to wait for affinity. Tries left: %d", maxTry));
                }
            }
        }

        if (!completed)
            LOG.error(String.format("Failed to wait for affinity for %d seconds. Will not wait any longer.",
                (100 * timeout) / 1000L));
    }

    /**
     * @return list of predefined caches.
     */
    protected List<String> getCacheNameList() {
        return new ArrayList<>(cacheNameList);
    }

    /**
     * @return list of predefined caches.
     */
    protected List<String> setCacheNameList() {
        if (ignite() == null)
            return new ArrayList<>();

        List<String> cacheNameList = new ArrayList<>();

        cacheNamesA = new ArrayList<>();
        cacheNamesB = new ArrayList<>();

        for (String cacheName : ignite().cacheNames()) {
            if (checkNameRange(cacheName)) {
                cacheNameList.add(cacheName);

                if(cacheName.contains("PART_A"))
                    cacheNamesA.add(cacheName);

                if(cacheName.contains("PART_B"))
                    cacheNamesB.add(cacheName);
            }
        }

        return cacheNameList;
    }

    /**
     * @param src Colon-separated strong e.g. "20:20:30"
     * @return Array of Integers int[]{20, 20, 30}
     */
    protected int[] getProbArray(String src) {
        String[] strArr = src.split(":");

        int[] probArr = new int[strArr.length];

        for (int i = 0; i < strArr.length; i++)
            probArr[i] = Integer.valueOf(strArr[i]);

        return probArr;
    }

    /**  */
    protected void addPropsToMap(TaskProperties props) {
        propMap.put(TaskProperties.TASK_NAME_PROPERTY, this.getClass().getSimpleName());

        for (Object prop : props.keySet())
            propMap.put(prop.toString(), props.get(prop).toString());
    }

    /**  */
    protected void printHelp() {

    }

    /**
     *
     * @param accending if {@code true}, then sort from oldest to youngest. Sort youngest to oldest otherwise.
     * @return A list of server nodes sorted by order in cluster.
     */
    protected List<ClusterNode> getServerNodesSorted(boolean accending) {
        List<ClusterNode> nodes = new ArrayList<>(getServerNodes());
        if (accending) {
            nodes.sort(Comparator.comparingLong(ClusterNode::order));
        } else {
            nodes.sort((o1, o2) -> Long.compare(o2.order(), o1.order()));
        }

        return nodes;
    }

    protected Collection<ClusterNode> getServerNodes() {
        return ignite().cluster().forServers().nodes();
    }

    protected Collection<ClusterNode> getClientNodes() {
        return ignite().cluster().forClients().nodes();
    }

    protected Collection<ClusterNode> getNodesByAttr(String attr, String val) {
        return ignite().cluster().forAttribute(attr, val).nodes();
    }

    protected List<NodeInfo> getNodesOldest(List<NodeInfo> nodes, int num) {
        nodes.sort(Comparator.comparingLong(NodeInfo::getOrder));

        List<NodeInfo> res = new LinkedList<>();

        for (NodeInfo node : nodes) {
            res.add(node);
            if (res.size() >= num)
                break;
        }

        return res;
    }

    protected List<NodeInfo> getNodesYoungest(List<NodeInfo> nodes, int num) {
        nodes.sort((o1, o2) -> Long.compare(o2.getOrder(), o1.getOrder()));

        List<NodeInfo> res = new LinkedList<>();

        for (NodeInfo node : nodes) {
            res.add(node);

            if (res.size() >= num)
                break;
        }

        return res;
    }

    protected List<NodeInfo> getNodesRandom(List<NodeInfo> nodes, int num) {
        List<NodeInfo> res = new LinkedList<>();

        Collections.shuffle(nodes);

        for (NodeInfo node : nodes) {
            res.add(node);

            if (res.size() >= num)
                break;
        }

        return res;
    }

    /**
     *
     * @param host Host.
     * @param consId Node consistent id.
     * @return Node process id related to node with the specified consistent id on the specified host.
     * @throws Exception If failed.
     */
    private String getPid(String host, String consId) throws Exception {
        SSHCmdWorker worker = new SSHCmdWorker(args);

        Map<String, String> pidMap = worker.getPidMap(host, null);

        String pid = null;

        for (String pid0 : pidMap.keySet())
            if (consId.equals(pidMap.get(pid0)))
                pid = pid0;

        return pid;
    }

    protected boolean waitIfStopFlag(String key) {
        Integer val;

        do {
            try {
                val = getMsgFlag(key);

                if (val == null) {
                    clearMsgFlag(key);
                    return true;
                }
            }
            catch (Exception e) {
                LOG.error(e);
                return false;
            }

            if (val == 0) {
                LOG.debug("Flag {} is zero. Resuming the task.", key);
                return true;
            }

            LOG.info("Flag {} has non-zero value {}. Waiting for 5 seconds.", key, val);
            sleep(5000L);

        } while (taskStartTime + timeToWork * 1000L > System.currentTimeMillis());

        LOG.error("Task work time is over. Will not wait any longer.");
        return false;
    }

    /**
     * Wait for all flags from {@code waitForFlags} to become true.
     * @return {@code false} on timeout
     */
    protected boolean checkWaitFlags() {
        boolean res = true;

        if (waitForFlags != null) {
            for (String flag : waitForFlags) {
                res = res && waitIfStopFlag(flag);
            }
        }

        return res;
    }

    /**
     *
     * @return {@code true} if a stop flag is found.
     */
    protected boolean stopOnStopFlags() {
        if (stopFlags != null) {
            Integer vals = 0;

            for (String flag : stopFlags) {
                Integer val;

                try {
                    val = getMsgFlag(flag);

                    if (val == null) {
                        clearMsgFlag(flag);
                        return false;
                    }

                    vals += val;
                }
                catch (Exception e) {
                    LOG.error(e);
                    return false;
                }

                LOG.debug("Stop flag {}, val: {}", flag, val);
            }

            return vals > 0;
        }

        return false;
    }

    protected void clearSyncFlags() {
        if (syncFlags != null)
            for (String flag : syncFlags)
                decMsgFlag(flag);
    }

    protected void raiseSyncFlags() {
        if (syncFlags != null)
            for (String flag : syncFlags)
                incMsgFlag(flag);
    }

    /**
     *
     */
    protected boolean isDataLost() {
        try {
            Integer val = getMsgFlag(DEFAULT_STOP_FLAG);

            if (val == null)
                return false;

            return val > 0;
        }
        catch (Exception e) {
            LOG.error("Failed to get value for dataLost flag", e);
        }
        return false;
    }

    /**
     *
     * @return Random server node host
     */
    public String getRandomServerHost() {
        List<String> servHosts = PocTesterUtils.getHostList(args.getServerHosts(), true);
        return servHosts.get(COMMON_RANDOM.nextInt(servHosts.size()));
    }

    protected Integer getMsgFlag(String key) throws Exception {
        return getMsgCache().get(key);
    }

    protected void setMsgFlag(String key, Integer val) {
        LOG.info("Setting flag {} to {}", key, val);

        IgniteCache<String, Integer> cache = getMsgCache();

        try (Transaction tx = ignite().transactions().txStart()) {
            cache.put(key, val);
            tx.commit();
        }
    }

    protected Integer incMsgFlag(String key) {
        LOG.info("Incrementing flag {}", key);

        IgniteCache<String, Integer> cache = getMsgCache();
        Integer val = null;

        // Start transaction
        try (Transaction tx = ignite().transactions().txStart()) {
            val = cache.get(key);

            if (val == null)
                val = 0;

            assert val >= 0;

            cache.put(key, val + 1);

            tx.commit();
        }
        catch (Exception e) {
            LOG.error(e);
            tearDown();
        }

        return val;
    }

    protected Integer decMsgFlag(String key) {
        LOG.info("Decrementing flag {}", key);

        IgniteCache<String, Integer> cache = getMsgCache();
        Integer val = null;

        try (Transaction tx = ignite().transactions().txStart()) {
            val = cache.get(key);

            if (val == null)
                val = 1; // Decrease will reset the value to zero

            assert val >= 0;

            cache.put(key, val - 1);
            tx.commit();
        }
        catch (Exception e) {
            LOG.error(e);
            tearDown();
        }

        return val;
    }

    protected void clearMsgFlag(String key) throws Exception {
        setMsgFlag(key, 0);
    }

    /**
     * Get cache for tasks synchronization
     * @return Instance of message cache
     */
    protected IgniteCache<String, Integer> getMsgCache() {
        IgniteCache<String, Integer> cache = null;

        try {
            cache = ignite().cache(MSG_CACHE_NAME);
        }
        catch (CacheException e) {
            LOG.error("Failed to get cache {}", MSG_CACHE_NAME);
            LOG.error(e);
            tearDown();
        }

        return cache;
    }

    protected void waitForTx(){
        int activeTx = activeTx();

        while (activeTx > 0) {
            LOG.info(String.format("Number of running transactions is %d. Waiting 5 seconds before next check.",
                activeTx));

            sleep(5000L);

            activeTx = activeTx();
        }
    }

    protected int activeTx(){
        Collection<Integer> resList = ignite.compute().broadcast(new TxCounter());

        int sum = 0;

        for(Integer res : resList)
            sum += res;

        return sum;
    }

    /**
     *
     */
    protected void createLockFile() {
        File file = new File(homeDir + SEP + lockFile);

        LOG.info("Creating lock file " + file.getAbsolutePath());

        try {
            boolean res = file.createNewFile();

            LOG.info("Lock file was created = " + res);
        }
        catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    
    protected long[] parseRangeLong(String str) {
        long[] res = new long[2];
        String[] strVals = str.split(":");
        
        if (strVals.length != 2) {
            throw new IllegalArgumentException(String.format("Invalid range %s", str));
        }
        
        try {
            res[0] = Long.valueOf(strVals[0]);
            res[1] = Long.valueOf(strVals[1]);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Invalid range %s", str));
        }
        
        return res;
    }
    
    protected int[] parseRangeInt(String str) {
        int[] res = new int[2];
        String[] strVals = str.split(":");

        if (strVals.length != 2) {
            throw new IllegalArgumentException(String.format("Invalid range %s", str));
        }

        try {
            res[0] = Integer.valueOf(strVals[0]);
            res[1] = Integer.valueOf(strVals[1]);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Invalid range %s", str));
        }

        return res;
    }

    /** */
    private void setTxLenRange() {
        if (txLen.contains(":")) {
            txLenCalcMode = TxLenCalcMode.RANDOM;

            String[] arr = txLen.split(":");

            if (arr.length > 1) {
                minTxLen = Integer.valueOf(arr[0]);
                txLenNum = Integer.valueOf(arr[1]);

                if (minTxLen >= txLenNum)
                    throw new IllegalArgumentException("Unexpected value for txLen.");
            }
            else
                txLenNum = Integer.valueOf(arr[0]);
        }
        else
            txLenNum = Integer.valueOf(txLen);
    }

    /**
     * @return arguments.
     */
    public PocTesterArguments getArgs() {
        return args;
    }

    /**
     *
     */
    protected void stopAndCollect() {
        synchronized (AbstractTask.class) {
            if (stopped)
                return;

            try {
                incMsgFlag("dataLost");
            }
            catch (Exception e) {
                LOG.error(e);
                throw e;
            }

//            LOG.info("Task will stop and copy work directories on server hosts.");

//            copyWorkDirs();

            stopped = true;

            tearDown();
        }
    }

    protected void copyWorkDirs() {
//        ignite().active(false);
//
//        while (ignite().active())
//            sleep(1000L);

        StatWorker statWorker = new StatWorker();

        String dt = dateTime();

        SSHCmdWorker worker = new SSHCmdWorker(args);

        List<String> dirsToCopy = new ArrayList<>();

        dirsToCopy.add(args.getRemoteWorkDir());

        if (args.getWalPath() != null)
            dirsToCopy.add(args.getWalPath());

        if (args.getWalArch() != null)
            dirsToCopy.add(args.getWalArch());

        LOG.info(String.format("Number of directories to backup is %d", dirsToCopy.size()));

        for (String host : PocTesterUtils.getHostList(args.getServerHosts(), true)) {
            try {
                statWorker.takeThreadDumps(args, host);
            }
            catch (Exception e) {
                LOG.error(String.format("Failed to take thread dumps on the host %s", host), e);

            }

            for (String dirToCopy : dirsToCopy) {
                String backUpDir = String.format("%s-backup-%s", dirToCopy, dt);

                LOG.info(String.format("Copying directory %s to %s", dirToCopy, backUpDir));

                String cpCmd = String.format("cp -r %s %s", dirToCopy, backUpDir);

                try {
                    worker.runCmd(host, cpCmd);
                }
                catch (Exception e) {
                    LOG.error(String.format("Failed to take thread dumps on the host %s", host), e);
                }
            }

            String suffixFileName = String.format("%s/backup-suffix.txt", args.getRemoteWorkDir());

            try {
                worker.runCmd(host, String.format("echo \"%s\" > %s", dt, suffixFileName));
            }
            catch (Exception e) {
                LOG.error(String.format("Failed to save backup-suffix.txt on the host %s", host), e);
            }
        }

//        ignite().active(true);
//
//        while (!ignite().active())
//            sleep(1000L);

    }

    /**
     * Gets total caches size.
     *
     * @return {@code long} Sum of caches size.
     */
    protected long getTotalCachesSize() {
        long totalCacheSize = 0;

        for (String cacheName : getCacheNameList())
            totalCacheSize += ignite().cache(cacheName).size();

        return totalCacheSize;
    }

    /**
     *
     */
    void takeThreadDump() {
        long currTime = System.currentTimeMillis();

        if (currTime - 30_000L > lastThreadDumpTime) {

            lastThreadDumpTime = currTime;

            StatWorker statWorker = new StatWorker();

            for (String host : PocTesterUtils.getHostList(args.getServerHosts(), true)) {
                try {
                    statWorker.takeThreadDumps(args, host);
                }
                catch (Exception e) {
                    LOG.error(String.format("Failed to take thread dumps on the host %s", host), e);
                }
            }
        }
    }

    /**
     * @param cacheNameSet {@code Set} of cache names.
     * @return {@code true} if no conflict found or {@code false} otherwise.
     * @throws IgniteCheckedException
     */
    protected boolean verifyBackupPartitions(Set<String> cacheNameSet) throws IgniteException {
        boolean done = false;

        int cnt = 0;

        while (!done && cnt++ < 40) {
            try {
                done = verify(cacheNameSet);
            }
            catch (IgniteException e) {
                LOG.error("Failed to verify backup partitions. Will try again after 30 seconds.", e);

                sleep(30_000L);
            }

            if (!done && cnt < 40) {
                LOG.error(String.format("Partition checksums are different. Will try to verify again after waiting " +
                    "30 seconds. Count = %d", cnt));

                LOG.info(String.format("Number of running transactions is %d", activeTx()));

                sleep(30_000L);
            }

            if (!done && cnt >= 10)
                LOG.error("Partition checksums are different. Will not try to verify again any more.");
        }

        return done;
    }

    /**
     * @param cacheNameSet {@code Set} of cache names.
     * @return {@code true} if no conflict found or {@code false} otherwise.
     * @throws IgniteCheckedException
     */
    protected boolean verify(Set<String> cacheNameSet) throws IgniteException {
        LOG.info(String.format("Starting to verify backup partitions for %d caches", cacheNameSet.size()));

        boolean done = false;

        int cnt = 0;

        ComputeTaskFuture<IdleVerifyResultV2> fut = null;

        while (!done && cnt++ < 10) {
            try {
                fut = ignite.compute().executeAsync(VerifyBackupPartitionsTaskV2.class,
                    new VisorIdleVerifyTaskArg(cacheNameSet));

                done = true;
            }
            catch (IgniteException e) {
                LOG.error("Failed to verify backup partitions. Will try again after 30 seconds.", e);

                sleep(30_000L);
            }
        }

        IdleVerifyResultV2 res = fut.get(300_000, TimeUnit.MILLISECONDS);

        if (res == null || !done) {
            LOG.error("Failed to verify backup partituons. Will not try any longer.");

            return false;
        }

        if (res.hasConflicts()) {
            LOG.error("Partition checksums are different for backups");

            Map<PartitionKeyV2, List<PartitionHashRecordV2>> conflicts = res.counterConflicts();

            if (!F.isEmpty(res.counterConflicts())) {
                LOG.info("Update counter conflicts:");

                for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : res.counterConflicts().entrySet()) {
                    LOG.info("Conflict partition: " + entry.getKey());
                    LOG.info("Partition instances: " + entry.getValue());
                }
            }

            if (!F.isEmpty(res.hashConflicts())) {
                LOG.info("Hash conflicts:");

                for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : res.hashConflicts().entrySet()) {
                    LOG.info("Conflict partition: " + entry.getKey());

                    LOG.info("Partition instances: " + entry.getValue());

                    int partId = entry.getKey().partitionId();
                    int grpId = entry.getKey().groupId();

                    GridCacheProcessor cacheProc = ((IgniteEx)ignite).context().cache();

                    List<GridCacheContext> cachesCtx = cacheProc.cacheGroup(grpId).caches();

                    for (boolean withKeepBinary : Arrays.asList(true, false)) {
                        for (GridCacheContext cacheCtx : cachesCtx) {
                            String cacheName = cacheCtx.name();

                            LOG.info("Cache name: {}, withKeepBinary: {}", cacheName, withKeepBinary);

                            boolean diffFound = printDifference(ignite, partId, cacheName, withKeepBinary);
                        }
                    }
                }
            }

            if (!F.isEmpty(res.movingPartitions())) {
                LOG.info("Verification was skipped for " + res.movingPartitions().size() + " MOVING partitions:");

                for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : res.movingPartitions().entrySet()) {
                    LOG.info("Rebalancing partition: " + entry.getKey());

                    LOG.info("Partition instances: " + entry.getValue());
                }
            }

            return false;

        }
        else
            LOG.info("Partition checksums are no different for backups.");

        return true;
    }

    private boolean checkWalRec(WALRecord record, long startTimeStamp, long duration) {
        if (record instanceof DataRecord || record instanceof TxRecord) {
            TimeStampRecord tsr = (TimeStampRecord)record;

            long ts = tsr.timestamp();

            return (ts > startTimeStamp && ts < startTimeStamp + duration);
        }

        return false;
    }

    /** */
    protected IgniteCache<Object, Object> getRandomCacheForContQuery() {
        int idx = COMMON_RANDOM.nextInt(4);

        IgniteCache<Object, Object> cache = null;

        switch (idx) {
            case 0:
                cache = ignite().cache("cont_query_tran_part");
                break;
            case 1:
                cache = ignite().cache("cont_query_tran_repl");
                break;
            case 2:
                cache = ignite().cache("cont_query_atom_part");
                break;
            case 3:
                cache = ignite().cache("cont_query_atom_repl");
        }

        return cache;
    }

    /**
     *
     */
    private class WalPrinter implements IgniteCallable<List<String>> {
        private long startTimeStamp;

        private long duration;

        /**
         * @param startTimeStamp
         * @param duration
         */
        public WalPrinter(long startTimeStamp, long duration) {
            this.startTimeStamp = startTimeStamp;
            this.duration = duration;
        }

        @Override public List<String> call() {
            LOG.info("Apply wal");

            try {
//                return parseWalRec(startTimeStamp, duration);
            }
            catch (Exception e) {
                LOG.error("WAL PRINTER", e);
            }

            return null;
        }
    }

    /**
     *
     * @param uniqKeys {@code Map} containing unique keys and cache names.
     * @return {@code List} containing unique keys.
     */
    protected List<Long> getKeyList(final Map<Long, String> uniqKeys){
        final List<Long> keyList = new ArrayList<>(uniqKeys.size());

        keyList.addAll(uniqKeys.keySet());

        Collections.sort(keyList);

        return keyList;
    }

    protected List<Long> randomUniqKeys(int num){
        List<Long> resList = new ArrayList<>();

        for (int i = 0; i < num; i++) {
            long key0 = getRandomKey();

            while (resList.contains(key0))
                key0 = getRandomKey();

            resList.add(key0);
        }

        Collections.sort(resList);

        return resList;
    }

    protected String[] getRandomCacheNamesPair() {
        String cacheName0;
        String cacheName1;

        GridCacheProcessor cacheProc = ((IgniteEx) ignite).context().cache();

        if (!mixedMode) {
            int cachesNum = getCacheNameList().size();

            cacheName0 = getCacheNameList().get(COMMON_RANDOM.nextInt(cachesNum));
            cacheName1 = getCacheNameList().get(COMMON_RANDOM.nextInt(cachesNum));

            if (cachePairSameBackups) {
                while (cacheProc.cacheConfiguration(cacheName0).getBackups() !=
                        cacheProc.cacheConfiguration(cacheName1).getBackups()) {
                    cacheName1 = getCacheNameList().get(COMMON_RANDOM.nextInt(cachesNum));
                }
            }
        }
        else {
            if (COMMON_RANDOM.nextBoolean()) {
                int pos = COMMON_RANDOM.nextInt(cacheNamesA.size());
                cacheName0 = cacheNamesA.get(pos);

                pos = COMMON_RANDOM.nextInt(cacheNamesB.size());
                cacheName1 = cacheNamesB.get(pos);
            }
            else {
                int pos = COMMON_RANDOM.nextInt(cacheNamesB.size());
                cacheName0 = cacheNamesB.get(pos);

                pos = COMMON_RANDOM.nextInt(cacheNamesA.size());
                cacheName1 = cacheNamesA.get(pos);
            }
        }

        assert !cachePairSameBackups || cacheProc.cacheConfiguration(cacheName0).getBackups() ==
                cacheProc.cacheConfiguration(cacheName1).getBackups()
                : "Number of backups in caches differ";

        return new String[] {cacheName0, cacheName1};
    }

    /**
     *
     * @param key Key.
     * @return Reversed key.
     */
    protected long reversedKey(long key){
        return (key * -1) - 1;
    }



    /** */
    protected void deployServices(String prefix){
        for (int i = 0; i < serviceNum; i++){

            // Get an instance of IgniteServices for the cluster group.
            boolean done = false;

            int cnt0 = 0;

            while (!done && cnt0++ < 300) {
                try {
                    svcs = ignite().services(ignite().cluster().forServers());                }
                catch (Exception e){
                    LOG.error(String.format("Failed to get services. Will try again. Cnt = %d", cnt0), e);

                    sleep(100L);
                }
            }

            boolean deployed = false;

            int cnt = 0;

            try {
                while (!deployed && cnt++ < 100) {

                    int idx = COMMON_RANDOM.nextInt(4);

                    switch (idx) {
                        case 0:
                            svcs.deployNodeSingleton(prefix + "myCounterService" + i, new PocServiceImpl());
                            break;
                        case 1:
                            svcs.deploy(new ServiceConfiguration().setName(prefix + "myCounterService" + i).setService(new PocServiceImpl()).setTotalCount(2));
                            break;
                        case 2:
                            svcs.deploy(new ServiceConfiguration().setName(prefix + "myCounterService" + i).setService(new PocServiceImpl()).setMaxPerNodeCount(2));
                            break;
                        case 3:
                            svcs.deployClusterSingleton(prefix + "myCounterService" + i, new PocServiceImpl());
                    }
                    deployed = true;
                }
            }
            catch (Exception e){
                LOG.error(String.format("Failed to deploy service. Will retry. Cnt = %d", cnt), e);

                sleep(500L);
            }
        }
    }

    /**
     *
     * @param threadName Thread name.
     * @return Thread index.
     */
    protected int extractThreadIndex(String threadName){
        int dashIdx = threadName.indexOf("-threadNum-");

        String idxStr = threadName.substring(dashIdx + 11, threadName.length());

        return Integer.valueOf(idxStr);
    }

    /**
     * Transforms poc-tester-server-XXX.XXX.XXX.XXX-id-X into poc_tester_server_XXX_XXX_XXX_XXX_id_X
     *
     * @param consId Consistent ID.
     * @return
     */
    protected String consIdToDirName(String consId){
        //TODO hanlde all symbols
        return consId.replaceAll("-", "_").replaceAll("\\.", "_");
    }

    /** */
    protected void cancelServices(String prefix){
        for (int i = 0; i < serviceNum; i++)
            svcs.cancel(prefix + "myCounterService" + i);
    }

    /** */
    protected enum TxLenCalcMode {
        /** */
        PREDEFINED,

        /** */
        RANDOM
    }

    /**
     * @param locNode Local node.
     * @param partId Partition id.
     * @param cacheName Cache name.
     */
    private boolean printDifference(Ignite locNode, int partId, String cacheName, boolean keepBinary) {
        IgniteClosure<T3<String, Integer, Boolean>, T2<String, Map<Object, Object>>> clo = new GetLocalPartitionKeys();

        List<T2<String /** Consistent id. */, Map<Object, Object> /** Entries. */>> res =
                new ArrayList<>(locNode.compute(locNode.cluster().forServers()).broadcast(clo, new T3<>(cacheName, partId, keepBinary)));

        // Remove same keys.
        Map<Object, Object> tmp = null;

        for (int i = 0; i < res.size(); i++) {
            T2<String, Map<Object, Object>> cur = res.get(i);

            if (tmp == null)
                tmp = new HashMap<>(cur.get2());
            else
                tmp.entrySet().retainAll(cur.get2().entrySet());
        }

        for (Map.Entry<Object, Object> entry : tmp.entrySet()) {
            for (T2<String, Map<Object, Object>> e : res)
                e.get2().remove(entry.getKey(), entry.getValue());
        }

        boolean diffFound = false;

        for (T2<String, Map<Object, Object>> e : res) {
            LOG.info("ConsistentId=" + e.get1() + ", entries=" + e.get2());

            if (!e.get2().isEmpty())
                diffFound = true;
        }

        return diffFound;
    }

    /** */
    public static final class GetLocalPartitionKeys implements IgniteClosure<T3<String, Integer, Boolean>, T2<String, Map<Object, Object>>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public T2<String, Map<Object, Object>> apply(T3<String, Integer, Boolean> arg) {
            T2<String, Map<Object, Object>> res = new T2<>((String) ignite.configuration().getConsistentId(), new HashMap<>());

            IgniteCache<Object, Object> cache = arg.get3() ? ignite.cache(arg.get1()).withKeepBinary() : ignite.cache(arg.get1());

            if (cache == null)
                return res;

            Iterable<Cache.Entry<Object, Object>> entries = cache.localEntries();

            for (Cache.Entry<Object, Object> entry : entries) {
                if (ignite.affinity(cache.getName()).partition(entry.getKey()) == arg.get2().intValue())
                    res.get2().put(entry.getKey(), entry.getValue());
            }

            return res;
        }
    }

}
