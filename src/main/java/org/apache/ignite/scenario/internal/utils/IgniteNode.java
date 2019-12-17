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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.scenario.internal.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;

import static org.apache.ignite.scenario.internal.utils.PocTesterUtils.println;

/**
 * Standalone Ignite node.
 */
public class IgniteNode {
    /** Grid instance. */
    private Ignite ignite;

    /** Client mode. */
    private boolean clientMode;

    /** */
    public IgniteNode() {
        // No-op.
    }

    /** */
    public IgniteNode(boolean clientMode) {
        this.clientMode = clientMode;
    }

    /** */
    public IgniteNode(boolean clientMode, Ignite ignite) {
        this.clientMode = clientMode;
        this.ignite = ignite;
    }

    /** */
    private static final Logger LOG = LogManager.getLogger(IgniteNode.class.getName());


    public static void main(String[] args) {

        PocTesterArguments args0 = new PocTesterArguments();
        args0.setArgsFromCmdLine(args);
        args0.setArgsFromStartProps();

        try {
            new IgniteNode().start(args0);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** {@inheritDoc} */
    public Ignite start(PocTesterArguments args) throws Exception {

        String cfgPath = clientMode ? args.getClientCfg() : args.getServerCfg();

        IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> tup = loadConfiguration(cfgPath);

        IgniteConfiguration c = tup.get1();

        assert c != null;

        ApplicationContext appCtx = tup.get2();

        assert appCtx != null;

        if(args.getStoragePath() != null) {
            if(c.getDataStorageConfiguration() != null) {
                println(String.format("Data storage path set to %s", args.getStoragePath()));

                c.getDataStorageConfiguration().setStoragePath(args.getStoragePath());
            }
        }

        if(args.getWalPath() != null) {
            if(c.getDataStorageConfiguration() != null) {
                println(String.format("WAL store path set to %s", args.getWalPath()));

                c.getDataStorageConfiguration().setWalPath(args.getWalPath());
            }
        }

        if(args.getWalArch() != null) {
            if(c.getDataStorageConfiguration() != null) {
                println(String.format("WAL archive path set to %s", args.getWalArch()));

                c.getDataStorageConfiguration().setWalArchivePath(args.getWalArch());
            }
        }
        CacheConfiguration[] ccfgs = c.getCacheConfiguration();

        if (ccfgs != null) {
            for (CacheConfiguration cc : ccfgs) {
//                if(cc.getName().equals("sqlFunc")) {
//                    LOG.info("Setting sql func");
//
//                    cc.setSqlFunctionClasses(IgniteSequenceFunction.class);
//
//                    for (Class cls :cc.getSqlFunctionClasses())
//                        LOG.info("After set " + cls.getCanonicalName());
//                }
                // IgniteNode can not run in CLIENT_ONLY mode,
                // except the case when it's used inside IgniteAbstractBenchmark.
//                boolean cl = args.isClientOnly() && (args.isNearCache() || clientMode);
//
//                if (cl)
//                    c.setClientMode(true);
//
//                if (args.isNearCache()) {
//                    NearCacheConfiguration nearCfg = new NearCacheConfiguration();
//
//                    int nearCacheSize = args.getNearCacheSize();
//
//                    if (nearCacheSize != 0)
//                        nearCfg.setNearEvictionPolicy(new LruEvictionPolicy(nearCacheSize));
//
//                    cc.setNearConfiguration(nearCfg);
//                }
//
//                if (args.cacheGroup() != null)
//                    cc.setGroupName(args.cacheGroup());
//
//                cc.setWriteSynchronizationMode(args.syncMode());

//                if(args.getBackups() != null)
//                    cc.setBackups(args.getBackups());

//                if (args.restTcpPort() != 0) {
//                    ConnectorConfiguration ccc = new ConnectorConfiguration();
//
//                    ccc.setPort(args.restTcpPort());
//
//                    if (args.restTcpHost() != null)
//                        ccc.setHost(args.restTcpHost());
//
//                    c.setConnectorConfiguration(ccc);
//                }
//
//                cc.setReadThrough(args.isStoreEnabled());
//
//                cc.setWriteThrough(args.isStoreEnabled());
//
//                cc.setWriteBehindEnabled(args.isWriteBehind());
//
//                LOG.info("Cache configured with the following parameters: " + cc);
            }
        }
        else
            LOG.info("There are no caches configured");

//        TransactionConfiguration tc = c.getTransactionConfiguration();
//
//        tc.setDefaultTxConcurrency(args.txConcurrency());
//        tc.setDefaultTxIsolation(args.txIsolation());
//
//        TcpCommunicationSpi commSpi = (TcpCommunicationSpi)c.getCommunicationSpi();
//
//        if (commSpi == null)
//            commSpi = new TcpCommunicationSpi();
//
//        c.setCommunicationSpi(commSpi);
//
//        if (args.getPageSize() != DataStorageConfiguration.DFLT_PAGE_SIZE) {
//            DataStorageConfiguration memCfg = c.getDataStorageConfiguration();
//
//            if (memCfg == null) {
//                memCfg = new DataStorageConfiguration();
//
//                c.setDataStorageConfiguration(memCfg);
//            }
//
//            memCfg.setPageSize(args.getPageSize());
//        }
//
//        if (args.persistentStoreEnabled()) {
//            DataStorageConfiguration pcCfg = new DataStorageConfiguration();
//
//            c.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));
//
//            c.setDataStorageConfiguration(pcCfg);
//        }

        ignite = IgniteSpring.start(c, appCtx);
//        if(!clientMode) {
//
//            LOG.info("cache1111111 " + ignite.cache("sqlFunc").toString());
//
//            Class[] cls1 = ignite.cache("sqlFunc").getConfiguration(CacheConfiguration.class).getSqlFunctionClasses();
//
//            LOG.info("cls1.length = " + cls1.length);
//
//            for (Class cls0 : cls1)
//                LOG.info(cls0.getCanonicalName());
//        }

        //BenchmarkUtils.println("Configured marshaller: " + ignite.cluster().localNode().attribute(ATTR_MARSHALLER));

        return ignite;
    }

    /**
     * @param springCfgPath Spring configuration file path.
     * @return Tuple with grid configuration and Spring application context.
     * @throws Exception If failed.
     */
    public static IgniteBiTuple<IgniteConfiguration, ? extends ApplicationContext> loadConfiguration(String springCfgPath)
        throws Exception {
        URL url;

        try {
            url = new URL(springCfgPath);
        }
        catch (MalformedURLException e) {
            url = IgniteUtils.resolveIgniteUrl(springCfgPath);

            if (url == null)
                throw new IgniteCheckedException("Spring XML configuration path is invalid: " + springCfgPath +
                    ". Note that this path should be either absolute or a relative local file system path, " +
                    "relative to META-INF in classpath or valid URL to IGNITE_HOME.", e);
        }

        GenericApplicationContext springCtx;

        try {
            springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(url));

            springCtx.refresh();
        }
        catch (BeansException e) {
            throw new Exception("Failed to instantiate Spring XML application context [springUrl=" +
                url + ", err=" + e.getMessage() + ']', e);
        }

        Map<String, IgniteConfiguration> cfgMap;

        try {
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new Exception("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null || cfgMap.isEmpty())
            throw new Exception("Failed to find ignite configuration in: " + url);

        System.setProperty(IgniteSystemProperties.IGNITE_CONFIG_URL, springCfgPath);

        return new IgniteBiTuple<>(cfgMap.values().iterator().next(), springCtx);
    }

//    /** {@inheritDoc} */
//    @Override public void stop() throws Exception {
//        Ignition.stopAll(true);
//    }
//
//    /** {@inheritDoc} */
//    @Override public String usage() {
//        return BenchmarkUtils.usage(new IgniteBenchmarkArguments());
//    }

    /**
     * @return Ignite.
     */
    public Ignite ignite() {
        return ignite;
    }
}
