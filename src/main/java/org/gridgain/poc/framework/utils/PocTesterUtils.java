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

package org.gridgain.poc.framework.utils;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.gridgain.poc.framework.worker.NodeType;
import org.gridgain.poc.framework.ssh.RemoteSshExecutor;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by oostanin on 09.12.17.
 */
public class PocTesterUtils {
    /** */
    private static final Logger LOG = LogManager.getLogger(PocTesterUtils.class.getName());

    protected static final Random COMMON_RANDOM = new Random();

    public static JCommander jcommander(String[] a, Object args) {
        JCommander jCommander = new JCommander();
        jCommander.setAcceptUnknownOptions(true);
        jCommander.addObject(args);
        jCommander.parse(a);
        return jCommander;
    }
    public static JCommander jcommander(String[] a, Object args, String programName) {
        JCommander jCommander = new JCommander();
        jCommander.setAcceptUnknownOptions(true);
        jCommander.setProgramName(programName);
        jCommander.addObject(args);
        jCommander.parse(a);
        return jCommander;
    }

    public static Properties loadProp(String relativePath) {
        Properties prop = new Properties();

        String pocHome = System.getProperty("pocTesterHome");

        String path = relativePath.startsWith(pocHome) ? relativePath : pocHome + File.separator + relativePath;

        try {
            prop.load(new FileInputStream(path));
        }
        catch (IOException e) {
            printw(String.format("Failed to load property file %s", relativePath));

            return null;
        }

        return prop;
    }

    public static String dateTime() {
        Date date = new Date(System.currentTimeMillis());

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

        return formatter.format(date);
    }

    public static String dateTime(Date date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

        return formatter.format(date);
    }

    public static String dateTime(long timeStamp) {
        Date date = new Date(timeStamp);

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss,SSS");

        return formatter.format(date);
    }

    public static Long getTimeStamp(String line) throws ParseException {
        String dateTime = line.substring(1, 24);

        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

        return format.parse(dateTime).getTime();
    }

    public static String readPropFromFile(String relativePath, String key) {
        Properties prop = loadProp(relativePath);

        String res = "";

        if (prop.getProperty(key) != null)
            res = prop.getProperty(key);
        else {
            printw(String.format("Failed to read property %s from file %s", key, relativePath));

            return null;
        }

        return res;
    }

    public static String getUnixRelPath(String locHome, String relPropPath) {
        Path fullPath = Paths.get(locHome, relPropPath);

        String locHomeName = new File(locHome).getName();

        File srcFile = fullPath.toFile();

        String srcFileName = srcFile.getName();

        String parentName = new File(srcFile.getParent()).getName();

//        while (!parentName.equals(locHomeName)){
//        }

        return String.format("%s/%s", parentName, srcFileName);
    }

    public static void printer(String str) {
        System.out.println(hms() + "[ERROR] " + str);

        LOG.error(str);
    }

    public static void printw(String str) {
        System.out.println(hms() + "[WARN ] " + str);

        LOG.warn(str);
    }

    public static void println(String str) {
        if (!str.startsWith(hms()))
            System.out.println(hms() + str);
        else
            System.out.println(str);

        LOG.info(str);
    }

    public static boolean isLocal(InetAddress addr) {
        // Check if the address is a valid special local or loop back
        if (addr.isAnyLocalAddress() || addr.isLoopbackAddress())
            return true;

        // Check if the address is defined on any interface
        try {
            return NetworkInterface.getByInetAddress(addr) != null;
        }
        catch (SocketException e) {
            return false;
        }
    }

    public static List<String> getAllHosts(PocTesterArguments args, boolean uniq) {
        List<String> res = new ArrayList<>();

        for (String host : getPocHosts(args, false))
            res.add(host);
        for (String host : getZooKeeperHosts(args, false))
            res.add(host);

        if (uniq)
            res = getUniqList(res);

        Collections.sort(res);

        return res;
    }

    public static List<String> getPocHosts(PocTesterArguments args, boolean uniq) {
        List<String> res = new ArrayList<>();

        for (String host : getPocServerHosts(args, false))
            res.add(host);
        for (String host : getPocClientHosts(args, false))
            res.add(host);

        if (uniq)
            res = getUniqList(res);

        return res;
    }

    public static List<String> getPocServerHosts(PocTesterArguments args, boolean uniq) {
        List<String> res = new ArrayList<>();

        if (args.getServerHosts() != null) {
            addHostsToList(res, args.getServerHosts());
            if (uniq)
                res = getUniqList(res);
            Collections.sort(res);
        }
        else
            LOG.warn("Server hosts are not defined!");

        return res;
    }

    public static List<String> getPocClientHosts(PocTesterArguments args, boolean uniq) {
        List<String> res = new ArrayList<>();

        if (args.getClientHosts() != null) {
            addHostsToList(res, args.getClientHosts());
            if (uniq)
                res = getUniqList(res);
            Collections.sort(res);
        }
        else
            LOG.warn("Client hosts are not defined!");

        return res;
    }

    public static List<String> getZooKeeperHosts(PocTesterArguments args, boolean uniq) {
        List<String> res = new ArrayList<>();

        if (args.getZooKeeperHosts() != null) {
            addHostsToList(res, args.getZooKeeperHosts());
            if (uniq)
                res = getUniqList(res);
            Collections.sort(res);
        }
        else
            LOG.warn("Zookeeper hosts is not defined!");

        return res;
    }

    public static String getRandomFromSequence(String seq) {
        String[] seqArr = seq.split(":");

        return seqArr[COMMON_RANDOM.nextInt(seqArr.length)];
    }

    public static boolean checkList(List<String> hostList) {
        if (hostList.size() == 0) {
            printer("No ip addresses is defined in property file or options.");

            return false;
        }

        return true;
    }

    private static void addHostsToList(List<String> list, String hosts) {
        if (hosts.equals("!"))
            return;

        for (String host : hosts.split(","))
            list.add(host);
    }

    private static List<String> getUniqList(List<String> list) {
        List<String> res = new ArrayList<>();
        Set<String> set = new HashSet<>();

        for (String s : list)
            set.add(s);

        for (String s : set)
            res.add(s);

        return res;
    }

    public static List<String> getHostList(String hosts, boolean uniq) {
        String[] hostArr = hosts.split(",");

        List<String> hostList = Arrays.asList(hostArr);

        List<String> res = uniq ? getUniqList(hostList) : hostList;

        Collections.sort(res);

        return res;
    }

    public static void saveJson(final String path, final Map<String, Object> map) throws IOException {

        final ObjectMapper mapper = new ObjectMapper();

        File parentDir = new File(path).getParentFile();

        if (!parentDir.exists())
            parentDir.mkdirs();

        final PrintWriter writer = new PrintWriter(new FileOutputStream(new File(path)));

        writer.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));

        writer.close();
    }

    public static void saveReport(final Path path, final Map<Object, Object> map) throws IOException {

        final ObjectMapper mapper = new ObjectMapper();

        final PrintWriter writer = new PrintWriter(new FileOutputStream(path.toFile()));

        writer.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));

        writer.close();
    }

    /**
     * Read map from a given json file.
     *
     * @param path {@code Path} to json file.
     * @return {@code Map}.
     */
    public static Map<Object, Object> readMap(Path path) {
        final ObjectMapper mapper = new ObjectMapper();

        Map<Object, Object> res = null;

        try {
            res = mapper.readValue(path.toFile(), HashMap.class);
        }
        catch (IOException e) {
            LOG.error("Failed to read file " + path, e);
        }

        return res;
    }

    public static void updateFile(Path path, List<String> resLines) throws IOException {
        File file = path.toFile();

        File parent = file.getParentFile();

        if(!parent.exists())
            parent.mkdirs();

        FileOutputStream outStream = new FileOutputStream(file, true);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream));

        for (int i = 0; i < resLines.size(); i++) {
            bw.write(resLines.get(i));

            if(i < resLines.size() - 1)
                bw.newLine();
        }

        bw.close();
    }

    public static void saveFile(Path path, List<String> resLines) throws IOException {
        File file = path.toFile();

        if (file.exists())
            file.delete();

        updateFile(path, resLines);
    }

    public static String hms() {
        DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
        String time = timeFormat.format(new Date(System.currentTimeMillis()));

        return "[" + time + "] ";
    }

    public static File[] getSubDirs(Path dir, final String prefix) {
        return dir.toFile().listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return prefix == null ? file.isDirectory() : file.isDirectory() && file.getName().startsWith(prefix);
            }
        });
    }

    /** */
    public static String findConsId(RemoteSshExecutor worker, String host, NodeType type) throws Exception {
        String prefix = "-DCONSISTENT_ID=poc-tester-" + type + "-" + host + "-id-";

//        System.out.println(hms() + "Checking consistent id on the host " + host);

        String pgrepCmd = "pgrep 'java'";

        List<String> pids = worker.runCmd(host, pgrepCmd);

        Set<String> consIDs = new HashSet<>();

        for (String pid : pids) {
            String psCmd = String.format("ps -p %s -o comm,args=ARGS", pid);

            List<String> argList = worker.runCmd(host, psCmd);

            for (String arg : argList) {
                String[] argArr = arg.split(" ");

                for (String a : argArr)
                    if (a.startsWith(prefix))
                        consIDs.add(a);
            }
        }

        int i = 0;

        for (i = 0; i < consIDs.size(); i++) {
            if (!consIDs.contains(prefix + i))
                return (prefix + i).replace("-DCONSISTENT_ID=", "");
        }

        String res = prefix + i;

        return res.replace("-DCONSISTENT_ID=", "");
    }

    /** */
    public static String findFreeJmxPort(RemoteSshExecutor worker, String host) throws Exception {
        String prefix = "-Dcom.sun.management.jmxremote.port=";

//        System.out.println(hms() + "Checking jmxport on the host " + host);

        String pgrepCmd = "pgrep 'java'";

        List<String> pids = worker.runCmd(host, pgrepCmd);

        List<String> portOpts = new ArrayList<>();

        for (String pid : pids) {
            String psCmd = String.format("ps -p %s -o comm,args=ARGS", pid);

            List<String> argList = worker.runCmd(host, psCmd);

            for (String arg : argList) {
                String[] argArr = arg.split(" ");

                for (String a : argArr)
                    if (a.startsWith(prefix))
                        portOpts.add(a);
            }
        }

        Collections.sort(portOpts);

        int i = 0;

        for (i = 0; i < portOpts.size(); i++) {
            if (!portOpts.get(i).equals(prefix + (1101 + i)))
                return (prefix + (1101 + i)).replace(prefix, "");
        }

        String res = prefix + (1101 + i);

        return res.replace(prefix, "");
    }

    public static String combineJvmOpts(String common, String jfr, String jmx, String logPath) {
        String logParentPath = new File(logPath).getParent();

        String crashDumpPath = String.format("%s/crash-dump-%s.log", logParentPath, dateTime());

        jfr = jfr == null ? "" : jfr;
        jmx = jmx == null ? "" : jmx;

        return String.format(" %s %s %s -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s " +
                "-XX:ErrorFile=%s -DpocLogFileName=%s",
            common, jfr, jmx, logParentPath, crashDumpPath, logPath);
    }

    /** */
    public static String getRemoteJavaHome(RemoteSshExecutor worker, String host) throws Exception {
        List<String> respond = worker.runCmd(host, "echo ${JAVA_HOME}");

        return !respond.isEmpty() ? respond.get(0) : null;
    }

    public static void createZip(String parent, String dstPath, List<String> filesToZip,
        List<String> foldersToZip) throws IOException {
        File zip = new File(dstPath);

        if (zip.exists())
            zip.delete();

        try {
            ZipFile zipFile = new ZipFile(dstPath);

            ArrayList<File> filesToAdd = new ArrayList<>();

            for (String file : filesToZip)
                filesToAdd.add(Paths.get(parent, file).toFile());

            ZipParameters parameters = new ZipParameters();

            parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);

            parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);

            for (File file : filesToAdd) {
                try {
                    zipFile.addFile(file, parameters);
                }
                catch (ZipException e) {
                    printer(String.format("Failed to add file %s to zip archive.", file.getAbsolutePath()));
                    printer(e.getMessage());
                }
            }

            for (String folder : foldersToZip) {
                Path folderPath = Paths.get(parent, folder);

                try {
                    zipFile.addFolder(folderPath.toFile(), parameters);
                }
                catch (ZipException e) {
                    printer(String.format("Failed to add directory %s to zip archive.", folderPath.toString()));
                    printer(e.getMessage());
                }
            }
        }
        catch (ZipException e) {
            e.printStackTrace();
        }
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getList(Set<String> set) {
        List<String> res = new ArrayList<>(set.size());

        for (String s : set)
            res.add(s);

        Collections.sort(res);

        return res;
    }

    public static void delete(String path) {
        File file = new File(path);

        if (file.exists())
            file.delete();
    }

    public static String getJfrOpts(PocTesterArguments args, String remoteLogPath, String pathSuffix, String rmtHome,
        String taskDirName) {
        String optStr = args.getFlightRec();

        int delay = 0;
        int dur = 60;

        try {
            String[] arr = optStr.split("\\D+");

            if (arr.length > 1) {
                delay = Integer.valueOf(arr[0]);
                dur = Integer.valueOf(arr[1]);
            }
            else
                dur = Integer.valueOf(arr[0]);
        }
        catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            printw(String.format("Failed to parse flight record option '%s'. Using default values (delay=0, " +
                "duration=60).", optStr));
        }

        String recPath = String.format("%s/jfr%s.jfr", remoteLogPath, pathSuffix);

        String recSetPath = taskDirName != null ?
            String.format("%s/%s/config/Profiling.jfc", rmtHome, taskDirName) :
            String.format("%s/config/Profiling.jfc", rmtHome);

        String opts = String.format("delay=%ds,duration=%ds,filename=%s,settings=%s", delay, dur, recPath, recSetPath);

        LOG.info(String.format("Java flight recording options are configured: [%s]", opts));

        String jfrOpts = String.format(" -XX:+UnlockCommercialFeatures -XX:+FlightRecorder " +
            "-XX:StartFlightRecording=%s", opts);

        return jfrOpts;
    }

    /**
     * Clean directory.
     *
     * @param path path to directory.
     */
    public static void cleanDirectory(String path) throws IOException {
        LinkedList<Path> paths = new LinkedList<>();

        appendOrRemove(paths, Files.newDirectoryStream(Paths.get(path)));

        while (!paths.isEmpty()) {
            if (Files.newDirectoryStream(paths.getLast()).iterator().hasNext())
                appendOrRemove(paths, Files.newDirectoryStream(paths.getLast()));
            else
                Files.delete(paths.removeLast());
        }
    }

    /**
     * Add path to the stack if path is directory, otherwise delete it.
     *
     * @param paths Stack of paths.
     * @param ds Stream of paths.
     */
    private static void appendOrRemove(LinkedList<Path> paths, DirectoryStream<Path> ds) throws IOException {
        for (Path p : ds) {
            if (Files.isDirectory(p))
                paths.add(p);
            else
                Files.delete(p);
        }
    }

//    /**
//     * Sets an environment variable FOR THE CURRENT RUN OF THE JVM
//     * Does not actually modify the system's environment variables,
//     *  but rather only the copy of the variables that java has taken,
//     *  and hence should only be used for testing purposes!
//     * @param key The Name of the variable to set
//     * @param value The value of the variable to set
//     */
//    @SuppressWarnings("unchecked")
//    public static <K,V> void setEnv(final String key, final String value) {
//        try {
//            /// we obtain the actual environment
//            final Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
//            final Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
//            final boolean environmentAccessibility = theEnvironmentField.isAccessible();
//            theEnvironmentField.setAccessible(true);
//
//            final Map<K,V> env = (Map<K, V>) theEnvironmentField.get(null);
//
//            if (SystemUtils.IS_OS_WINDOWS) {
//                // This is all that is needed on windows running java jdk 1.8.0_92
//                if (value == null)
//                    env.remove(key);
//                else
//                    env.put((K) key, (V) value);
//
//            } else {
//                // This is triggered to work on openjdk 1.8.0_91
//                // The ProcessEnvironment$Variable is the key of the map
//                final Class<K> variableClass = (Class<K>) Class.forName("java.lang.ProcessEnvironment$Variable");
//                final Method convertToVariable = variableClass.getMethod("valueOf", String.class);
//                final boolean conversionVariableAccessibility = convertToVariable.isAccessible();
//                convertToVariable.setAccessible(true);
//
//                // The ProcessEnvironment$Value is the value fo the map
//                final Class<V> valueClass = (Class<V>) Class.forName("java.lang.ProcessEnvironment$Value");
//                final Method convertToValue = valueClass.getMethod("valueOf", String.class);
//                final boolean conversionValueAccessibility = convertToValue.isAccessible();
//                convertToValue.setAccessible(true);
//
//                if (value == null) {
//                    env.remove(convertToVariable.invoke(null, key));
//                } else {
//                    // we place the new value inside the map after conversion so as to
//                    // avoid class cast exceptions when rerunning this code
//                    env.put((K) convertToVariable.invoke(null, key), (V) convertToValue.invoke(null, value));
//
//                    // reset accessibility to what they were
//                    convertToValue.setAccessible(conversionValueAccessibility);
//                    convertToVariable.setAccessible(conversionVariableAccessibility);
//                }
//            }
//            // reset environment accessibility
//            theEnvironmentField.setAccessible(environmentAccessibility);
//
//            // we apply the same to the case insensitive environment
//            final Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
//            final boolean insensitiveAccessibility = theCaseInsensitiveEnvironmentField.isAccessible();
//            theCaseInsensitiveEnvironmentField.setAccessible(true);
//            // Not entirely sure if this needs to be casted to ProcessEnvironment$Variable and $Value as well
//            final Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
//            if (value == null) {
//                // remove if null
//                cienv.remove(key);
//            } else {
//                cienv.put(key, value);
//            }
//            theCaseInsensitiveEnvironmentField.setAccessible(insensitiveAccessibility);
//        } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
//            throw new IllegalStateException("Failed setting environment variable <"+key+"> to <"+value+">", e);
//        } catch (final NoSuchFieldException e) {
//            // we could not find theEnvironment
//            final Map<String, String> env = System.getenv();
//            Stream.of(Collections.class.getDeclaredClasses())
//                // obtain the declared classes of type $UnmodifiableMap
//                .filter(c1 -> "java.util.Collections$UnmodifiableMap".equals(c1.getName()))
//                .map(c1 -> {
//                    try {
//                        return c1.getDeclaredField("m");
//                    } catch (final NoSuchFieldException e1) {
//                        throw new IllegalStateException("Failed setting environment variable <"+key+"> to <"+value+"> when locating in-class memory map of environment", e1);
//                    }
//                })
//                .forEach(field -> {
//                    try {
//                        final boolean fieldAccessibility = field.isAccessible();
//                        field.setAccessible(true);
//                        // we obtain the environment
//                        final Map<String, String> map = (Map<String, String>) field.get(env);
//                        if (value == null) {
//                            // remove if null
//                            map.remove(key);
//                        } else {
//                            map.put(key, value);
//                        }
//                        // reset accessibility
//                        field.setAccessible(fieldAccessibility);
//                    } catch (final ConcurrentModificationException e1) {
//                        // This may happen if we keep backups of the environment before calling this method
//                        // as the map that we kept as a backup may be picked up inside this block.
//                        // So we simply skip this attempt and continue adjusting the other maps
//                        // To avoid this one should always keep individual keys/value backups not the entire map
//                        LOG.info("Attempted to modify source map: "+field.getDeclaringClass()+"#"+field.getName(), e1);
//                    } catch (final IllegalAccessException e1) {
//                        throw new IllegalStateException("Failed setting environment variable <"+key+"> to <"+value+">. Unable to access field!", e1);
//                    }
//                });
//        }
//        LOG.info("Set environment variable <"+key+"> to <"+value+">. Sanity Check: "+System.getenv(key));
//    }

    public static String resolveRelName(String homeDir, String name) {
        if (name.startsWith(homeDir))
            return name;
        else
            return Paths.get(homeDir, name).toString();
    }

    public static int getSelfPid() throws Exception {
        java.lang.management.RuntimeMXBean runtime =
            java.lang.management.ManagementFactory.getRuntimeMXBean();
        java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
        jvm.setAccessible(true);
        sun.management.VMManagement mgmt =
            (sun.management.VMManagement)jvm.get(runtime);
        java.lang.reflect.Method pid_method =
            mgmt.getClass().getDeclaredMethod("getProcessId");
        pid_method.setAccessible(true);

        int pid = (Integer)pid_method.invoke(mgmt);

        return pid;
    }

    public static String getHostFromConsId(String consId) {
        int idx1 = consId.indexOf("-id-");

        return consId.substring(18, idx1);
    }

    public static List<String> combine() {
        List<String> res = new ArrayList<>();
        for (int j = 1; j <= 100; j++) {
            for (int i = 0; i < 10; i++) {

                String type = i % 2 == 0 ? "A" : "B";

                int group = i % 2;

                res.add("<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">");
                res.add(String.format("    <property name=\"name\" value=\"cachepoc_PART_%s_%d\"/>", type, ((j - 1) * 10) + i));
                res.add(String.format("    <property name=\"groupName\" value=\"cache_group_%d\"/>", group));
                res.add("    <property name=\"atomicityMode\" value=\"TRANSACTIONAL\"/>");
                res.add("    <property name=\"backups\" value=\"2\"/>");
                res.add("    <property name=\"cacheMode\" value=\"PARTITIONED\"/>");
                res.add("    <property name=\"writeSynchronizationMode\" value=\"FULL_SYNC\"/>");
                res.add("    <property name=\"partitionLossPolicy\" value=\"READ_WRITE_SAFE\"/>");
                res.add("</bean>");

                res.add("");
            }

        }

        return res;

    }

    public static List<String> combine5() {
        List<String> res = new ArrayList<>();

//        String[] letters = {"A", "B"};
        String[] letters = {"A", "B", "C", "D"};
        String letter;
        Map<String, Integer> lettersCntr = new HashMap<>();

        for (String l : letters)
            lettersCntr.put(l, 0);

        String[] atomicityMode = {"ATOMIC", "TRANSACTIONAL", "TRANSACTIONAL_SNAPSHOT"};
        String[] backups = {"1", "2", "3"};
        String[] regionName = {"inMemoryConfiguration", "persistenceConfiguration"};
        String[] drSenderGrp = {"1", "2"};

        boolean drEnabled = false;

        int groups = 50 * letters.length;
        int cachesPerGroup = 10;

        int n = 0;

        for (int g = 0; g < groups; g++) {
            for (int i = 0; i < cachesPerGroup; i++) {

                if (letters.length == 4) {
                    int rem = g % 4;

                    if (rem == 0 || rem == 1)
                        letter = letters[rem];
                    else
                        letter = letters[2 + i % 2];
                }
                else {
                    letter = letters[g % 2];
                }

                int cntr = lettersCntr.get(letter);

                String regName = String.format("${cache.dataRegionName.%d}", g % regionName.length);
                if (letter.equals("C") || letter.equals("D"))
                    regName = "persistenceConfiguration";

                res.add("<bean class=\"org.apache.ignite.configuration.CacheConfiguration\">");
                res.add(String.format("    <property name=\"name\" value=\"cachepoc_PART_%s_%d\"/>", letter, cntr));
                res.add(String.format("    <property name=\"groupName\" value=\"cache_group_%d\"/>", n));
                res.add(String.format("    <property name=\"atomicityMode\" value=\"${cache.atomicityMode.%d}\"/>", g % atomicityMode.length));
                res.add(String.format("    <property name=\"backups\" value=\"${cache.backups.%d}\"/>", g % backups.length));
                res.add(String.format("    <property name=\"cacheMode\" value=\"PARTITIONED\"/>"));
                res.add(String.format("    <property name=\"dataRegionName\" value=\"%s\"/>", regName));
                res.add("    <property name=\"writeSynchronizationMode\" value=\"FULL_SYNC\"/>");
                res.add("    <property name=\"partitionLossPolicy\" value=\"${cache.partitionLossPolicy}\"/>");

                if (i % 3 == 0)
                    res.add("    <property name=\"indexedTypes\" ref=\"indexed-types\"/>");

                if (drEnabled)
                    res.add(String.format("    <property name=\"pluginConfigurations\" ref=\"dr-sender-grp-%d\"/>", g % drSenderGrp.length + 1));

                res.add("</bean>");

                res.add("");

                lettersCntr.put(letter, cntr + 1);

                n++;
            }
        }

        return res;
    }

    public static List<String> combine1() {
        String dirPref = "";


        List<String> scr = new ArrayList<>();
        List<String> res = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(dirPref + "config/cluster/ignite-1000-SYNC-config.xml"))) {
            String line;
            while ((line = br.readLine()) != null)
                scr.add(line);

        }
        catch (IOException e) {
            e.printStackTrace();
        }

        for (String line : scr) {

            if (line.contains("<property name=\"writeSynchronizationMode\" value=\"FULL_SYNC\"/>")) {
                res.add(line);
                res.add("                    <property name=\"affinity\">");
                res.add("                        <bean class=\"org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction\">");
                res.add("                            <constructor-arg value=\"false\"/>");
                res.add("                            <constructor-arg value=\"4096\"/>");
                res.add("                        </bean>");
                res.add("                    </property>");
            }
            else
                res.add(line);

        }

        return res;
    }

    public static List<String> combine2() {
        String dirPref = "";

        String clusterDir = dirPref + "config/cluster";

        File clusterDirFile = new File(clusterDir);

        File[] fileArr = clusterDirFile.listFiles();

        for (File file : fileArr) {
            if (file.isDirectory())
                continue;

            if (file.getName().contains("server") || file.getName().contains("client"))
                continue;

            if (file.getName().contains("log4j2.xml"))
                continue;

            List<String> newList = new ArrayList<>();

            try (BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    newList.add(line);

                    if (line.contains("property name=\"groupName\" value=\"cache_group")) {
                        int idx = line.indexOf("\"/>");

                        String n = line.substring(idx - 1, idx);

                        int n0 = Integer.valueOf(n);

                        if (n0 % 2 == 0) {
                            newList.add("                    <property name=\"nodeFilter\">");
                            newList.add("                        <bean class=\"org.gridgain.poc.filters.OldestNodeFilter\"/>");
                            newList.add("                    </property>");

                        }
                    }
                }

                file.delete();

                updateFile(file.toPath(), newList);

            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public static List<String> combine3() {

        List<String> dirList = new ArrayList<>();
        
        String dirPref = "";

        dirList.add(dirPref + "config");
        dirList.add(dirPref + "config/check-restore");
        dirList.add(dirPref + "config/jdbc");
        dirList.add(dirPref + "config/transfer");
        dirList.add(dirPref + "config/jdbc.mvcc");

        for (String dirPath : dirList) {

            File dir = new File(dirPath);

            File[] fileArr = dir.listFiles();

            List<File> fileList = new ArrayList<>();

            for (File file : fileArr) {
                if (file.isDirectory())
                    continue;

                if (file.getName().startsWith("Prof"))
                    continue;

                if (!file.getName().endsWith(".properties"))
                    continue;

                if (file.getName().startsWith("qaoo")) {
                    file.delete();

                    continue;
                }

                boolean add = true;

                if (add)
                    fileList.add(file);
            }

            for (File file : fileList) {
                if (file.isDirectory())
                    continue;

                if (file.getName().contains("log4j2.xml"))
                    continue;

                List<String> newList = new ArrayList<>();

                try (BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()))) {
                    String line;

                    String prevLine = "";
                    while ((line = br.readLine()) != null) {
                        if (!prevLine.startsWith("#")) {
                            if (line.contains("MAIN_CLASS"))
                                newList.add("# Task main class.");

                            if (line.contains("timeToWork="))
                                newList.add("# Time to work.");

                            if (line.contains("interval="))
                                newList.add("# Interval between iterations.");

                            if (line.contains("reportInterval="))
                                newList.add("# Report interval.");

                            if (line.contains("cacheNamePrefix="))
                                newList.add("# Cache name prefix.");

                            if (line.contains("threads="))
                                newList.add("# Task threads count.");

                            if (line.contains("cacheRangeStart="))
                                newList.add("# Cache range start.");

                            if (line.contains("cacheRangeEnd="))
                                newList.add("# Cache range end.");

                            if (line.contains("dataRange="))
                                newList.add("# Load range.");

                            if (line.contains("waitBeforeStart="))
                                newList.add("# Time interval between starting the node and starting actual test.");

                            if (line.contains("loadThreads="))
                                newList.add("# Load threads count.");

                            if (line.contains("lockFile="))
                                newList.add("# Lock file name.");

                            if (line.contains("txLen="))
                                newList.add("# Number of steps in transaction.");

                            if (line.contains("connectionString="))
                                newList.add("# Connection string. If no host ip is defined ip for connection will " +
                                    "be chosen randomly from server host ip list.");

                            if (line.contains("offTime="))
                                newList.add("# Time interval for node to be offline.");

                            if (line.contains("numToRestart="))
                                newList.add("# Number of nodes to restart during one iteration. Cannot be equal or " +
                                    "more then total number of server nodes in cluster.");

                            if (line.contains("checkFlag="))
                                newList.add("# Flag for checking msgCache flags.");

                            if (line.contains("txConcurrency="))
                                newList.add("# Transaction concurrency.");

                            if (line.contains("txIsolation="))
                                newList.add("# Transaction isolation.");
                        }

                        newList.add(line);

                        prevLine = line;
                    }

                    file.delete();



                    updateFile(file.toPath(), newList);

                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;
    }

    public static void addLineToCfg() throws IOException {
        String dirPref = "";


        File[] arr = new File(dirPref + "config/cluster").listFiles();

        for (File cfg : arr) {
            if (cfg.isDirectory())
                continue;

            String newName = cfg.getAbsolutePath() + "temp";

            Files.move(cfg.toPath(), Paths.get(newName), StandardCopyOption.REPLACE_EXISTING);

            updateFile(cfg.toPath(), combine2(newName));

            new File(newName).delete();

        }
    }

    public static List<String> combine2(String path) {
        List<String> scr = new ArrayList<>();
        List<String> res = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null)
                scr.add(line);

        }
        catch (IOException e) {
            e.printStackTrace();
        }

        for (String line : scr) {

            if (line.contains("<property name=\"writeSynchronizationMode\" value=\"FULL_SYNC\"/>")) {
                res.add(line);
                res.add("                    <property name=\"partitionLossPolicy\" value=\"READ_WRITE_SAFE\"/>");
            }
            else
                res.add(line);

        }

        return res;
    }

}


