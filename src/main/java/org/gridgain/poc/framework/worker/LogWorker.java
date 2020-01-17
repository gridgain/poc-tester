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

package org.gridgain.poc.framework.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.gridgain.poc.framework.worker.charts.LogParserContext;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import static org.gridgain.poc.framework.utils.PocTesterUtils.dateTime;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;
import static org.gridgain.poc.framework.utils.PocTesterUtils.printw;

/**
 * Parses log files and creates summary log files.
 */
public class LogWorker extends AbstractWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(LogWorker.class.getName());

    /** */
    private LogParserContext ctx;

    /** */
    private List<String> summaryList;

    /** */
    private List<String> servExclList;

    /** */
    private String analizedLogDirName;

    /** */
    private File mainDir;

    /** Map containing cache names and partitions with lost keys. */
    private Map<String, Set<Long>> cachePartMap;

    /** */
    private Set<String> logSet;

    /** */
    private List<String> misValList;

    /** */
    private List<ParsedLog> parsedList;

    /** */
    private Map<String, ParsedLog> logMap;

    /** */
    private Set<String> subjSet;

    /** */
    private Map<String, String> lastLines;

    /**
     *
     */
    private LogWorker() {
        cachePartMap = new HashMap<>();

        logSet = new TreeSet<>();

        misValList = new ArrayList<>();
    }

    /**
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        LogWorker creator = new LogWorker();

        creator.analyze(args);
    }

    /**
     * Print help.
     */
    @Override protected void printHelp() {
        System.out.println(" Parses log files.");
        System.out.println();
        System.out.println(" Usage:");
        System.out.println(" ./bin/include/log-analyzer.sh <options>");
        System.out.println();
        System.out.println(" Options:");
        System.out.println(" -h   || --help                Print help");
        System.out.println(" -rd  || --resDir              Result directory. If not defined newest log directory will be used.");
        System.out.println(" -lac || --logAnalyzerConfig   Path to configuration file. Default: " +
            "config/log-analyzer-cfg/log-analyzer-context.yaml");
        System.out.println();
    }

    /**
     * @param argsArr Command line arguments.
     */
    private void analyze(String[] argsArr) {
        args = new PocTesterArguments();

        locHome = System.getProperty("user.dir");

        try {
            PocTesterUtils.jcommander(argsArr, args, "log-worker");
        }
        catch (Exception e) {
            LOG.error(String.format("Failed to parse command line arguments: %s", e.getMessage()));

            printHelp();

            System.exit(1);
        }

        if (args.isHelp()) {
            printHelp();

            System.exit(0);
        }

        String resDirPath;

        if (args.getResDir() != null)
            resDirPath = args.getResDir();
        else {
            printw("Result directory is not defined. Will try to create report for the newest log directory.");

            resDirPath = getNewestLogDir();
        }

        if (resDirPath == null) {
            printer("Result directory is not found.");

            System.exit(1);
        }

        mainDir = new File(resDirPath);

        analizedLogDirName = "analyzed-logs-" + dateTime();

        parseYaml();

        setTaskMap();

        parseServerLogs();
    }

    /**
     *
     */
    private void parseYaml() {
        String yamlPath = args.getLogAnalyzerConfig();

        if (!new File(yamlPath).exists())
            yamlPath = Paths.get(System.getProperty("user.dir"), args.getLogAnalyzerConfig()).toString();

        if (!new File(yamlPath).exists()) {
            LOG.error(String.format("failed to find property file '%s'.", yamlPath));

            System.exit(1);
        }

        Yaml yaml = new Yaml(new Constructor(LogParserContext.class));

        try {
            ctx = yaml.load(new FileInputStream(yamlPath));

            if (ctx == null
                || ctx.getClientSummaryCtx() == null
                || ctx.getClientSummaryCtx().getLinesToParse() == null
                || ctx.getServerEventCtx() == null
                || ctx.getServerEventCtx().getEvents() == null
                || ctx.getServerExcluded() == null) {
                LOG.info("Failed to load log analyzer configuration.");

                System.exit(1);
            }

            summaryList = ctx.getClientSummaryCtx().getLinesToParse();

            servExclList = ctx.getServerExcluded();
        }
        catch (FileNotFoundException ignored) {
            LOG.info(String.format("Failed to find file %s.", yamlPath));

            System.exit(1);
        }
    }

    /**
     *
     */
    private void parseServerLogs() {
        setLogMap();

        List<File> logList = getServerLogList();

        for (File file : logList) {
            try {
                parseLog(file);
            }
            catch (IOException e) {
                LOG.error(String.format("Failed to copy file %s", file.getAbsoluteFile()), e);
            }
        }

        saveParsed();
    }

    /**
     *
     */
    private void setLogMap() {
        logMap = new HashMap<>();

        parsedList = new ArrayList<>();

        subjSet = new HashSet<>();

        lastLines = new HashMap<>();

        Set<String> propSet = ctx.getServerEventCtx().getEvents().keySet();

        for (String keyStr : propSet) {
            subjSet.add(keyStr);

            List<String> toParseList = ctx.getServerEventCtx().getEvents().get(keyStr);

            parsedList.add(new ParsedLog(keyStr, toParseList));

            if (!toParseList.isEmpty())
                lastLines.put(keyStr, toParseList.get(toParseList.size() - 1));
        }
    }

    /**
     *
     */
    private void saveParsed() {
        for (String consId : logMap.keySet()) {
            for (String subj : logMap.get(consId).keySet()) {
                Path subjLogDir = Paths.get(mainDir.getAbsolutePath(), analizedLogDirName, subj);

                List<String> logList = logMap.get(consId).get(subj);

                Collections.sort(logList);

                List<String> toSave = new ArrayList<>();

                for (String logStr : logList) {
                    toSave.add(logStr);

                    String last = lastLines.get(subj);

                    if (last != null && logStr.contains(last))
                        toSave.add("");
                }

                Path res = Paths.get(subjLogDir.toString(), consId + ".log");

                try {
                    if (!toSave.isEmpty())
                        PocTesterUtils.updateFile(res, toSave);
                    else
                        printw(String.format("No '%s' related lines in '%s' node log files.", subj, consId));
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     */
    private void setTaskMap() {
        List<File> fileList = getTaskLogList();

        for (File logFile : fileList) {
            try {
                parse(logFile);
            }
            catch (IOException e) {
                LOG.error(String.format("Failed to parse file %s", logFile.getAbsolutePath()), e);
            }
        }

        List<String> cacheList = new ArrayList<>(cachePartMap.keySet());

        Collections.sort(cacheList);

        List<String> toMissedCacheFile = new ArrayList<>();

        for (String cache : cacheList) {
            StringBuilder sb = new StringBuilder();

            Set<Long> parts = cachePartMap.get(cache);

            List<Long> partList = new ArrayList<>(parts);

            Collections.sort(partList);

            sb.append(String.format("%s -> %d    [ ", cache, cachePartMap.get(cache).size()));

            for (Long part : partList)
                sb.append(String.format(" %d ", part));

            sb.append(" ]");

            toMissedCacheFile.add(sb.toString());
        }

        List<String> logList = new ArrayList<>(logSet);

        Collections.sort(logList);

        List<String> filtList = new ArrayList<>();

        for (int i = 0; i < logList.size(); i++) {
            if ((filtList.isEmpty() || !filtList.get(filtList.size() - 1).isEmpty()) && (logList.get(i).contains("Creating") || logList.get(i).contains("Setting")
                || logList.get(i).contains("Started restore") || logList.get(i).contains("Starting")))
                filtList.add("");

            if ((filtList.isEmpty() || !filtList.get(filtList.size() - 1).isEmpty()) && (i == 0 || (!logList.get(i - 1).contains("illing node") && logList.get(i).contains("illing node"))))
                filtList.add("");

            //Add empty line if filtered list is empty or previous line in the list is not empty
            if ((filtList.isEmpty() || !filtList.get(filtList.size() - 1).isEmpty())
                // AND previous line does not contain substring "Topology snapshot"
                && (i == 0 || (!logList.get(i - 1).contains("Topology snapshot")
                // AND current line contains substring "Topology snapshot"
                && logList.get(i).contains("Topology snapshot"))))
                filtList.add("");

            if (i == 0 || !(logList.get(i - 1).contains("Found stop flag for key 'pauseTX'")
                && logList.get(i).contains("Found stop flag for key 'pauseTX'"))
                && !(logList.get(i - 1).substring(25, logList.get(i - 1).length() - 1).equals(logList.get(i).substring(25, logList.get(i).length() - 1))))
                filtList.add(logList.get(i));

            if (i == logList.size() - 1 || (logList.get(i).contains("illing node") && !logList.get(i + 1).contains("illing node")))
                filtList.add("");

            if (i == logList.size() - 1 || (logList.get(i).contains("Topology snapshot") && !logList.get(i + 1).contains("Topology snapshot")))
                filtList.add("");
        }

        Collections.sort(misValList);

        File missedCachesFile = Paths.get(mainDir.getAbsolutePath(), analizedLogDirName, "missed-caches.log").toFile();

        File missedValFile = Paths.get(mainDir.getAbsolutePath(), analizedLogDirName, "missed-values.log").toFile();

        File summaryFile = Paths.get(mainDir.getAbsolutePath(), analizedLogDirName, "summary.log").toFile();

        missedCachesFile.delete();

        summaryFile.delete();

        try {
            PocTesterUtils.updateFile(summaryFile.toPath(), filtList);

            if (!toMissedCacheFile.isEmpty())
                PocTesterUtils.updateFile(missedCachesFile.toPath(), toMissedCacheFile);

            if (!misValList.isEmpty())
                PocTesterUtils.updateFile(missedValFile.toPath(), misValList);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param file File to parse.
     * @throws IOException If failed.
     */
    private void parse(File file) throws IOException {
        if (file.getName().startsWith("gc-"))
            return;

        if (file.getName().endsWith(".gz")) {
            File tempDecompressedFile = new File(file.getAbsolutePath().replace(".gz", ""));

            unzipFile(file.getAbsolutePath(), tempDecompressedFile.getAbsolutePath());

            setMap(tempDecompressedFile);

            file.delete();
        }
        else
            setMap(file);
    }

    /**
     * @param file File to parse.
     * @throws IOException If failed.
     */
    private void parseLog(File file) throws IOException {
        String fileName = file.getName();

        if (fileName.startsWith("gc-"))
            return;

        if (fileName.endsWith(".gz")) {
            File tempDecompressedFile = new File(file.getAbsolutePath().replace(".gz", ""));

            unzipFile(file.getAbsolutePath(), tempDecompressedFile.getAbsolutePath());

            cleanAndCopy(tempDecompressedFile);

            file.delete();
        }
        else
            cleanAndCopy(file);
    }

    /**
     * @param file File to parse.
     * @throws IOException If failed.
     */
    private void cleanAndCopy(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String fileName = file.getName();

        String consId = getConsId(fileName);

        if (!logMap.containsKey(consId)) {
            ParsedLog pLog = new ParsedLog(null, null);

            for (String key : subjSet)
                pLog.put(key, new ArrayList<>());

            logMap.put(consId, pLog);
        }

        String line;

        List<String> toResFile = new ArrayList<>();

        while ((line = reader.readLine()) != null) {
            if (!line.startsWith("[2"))
                continue;

            if (!excluded(line))
                toResFile.add(line);

            for (ParsedLog pl : parsedList) {
                for (String subj : pl.keySet()) {
                    List<String> toLookList = pl.get(subj);
                    for (String toLook : toLookList) {
                        if (line.contains(toLook))
                            logMap.get(consId).get(subj).add(line);
                    }
                }
            }
        }

        String repl = analizedLogDirName + "/servers-without-excluded";

        Path resPath = Paths.get(file.getAbsolutePath().replace("servers", repl));

        if (!toResFile.isEmpty())
            PocTesterUtils.updateFile(resPath, toResFile);
    }

    /**
     * Converts file name to node consistent id.
     *
     * @param fileName File name.
     * @return Consistent id.
     */
    private String getConsId(String fileName) {
        int idIdx = fileName.indexOf("-id-");

        String afterId = fileName.substring(idIdx + 4);

        int delIdx = afterId.indexOf('-');

        return fileName.substring(0, idIdx + 4 + delIdx);
    }

    /**
     * Fills 'cachePartMap' in case some logs contains lines like 'Failed to find value for key K'.
     *
     * @param file File to parse.
     * @throws IOException If failed.
     */
    private void setMap(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line;

        while ((line = reader.readLine()) != null) {
            if (!line.startsWith("[2"))
                continue;

            // Parse lines "Failed to find value for key %d in cache %s"
            // and "Failed to find value for key %d and its reversed key %d in cache %s"
            if (line.contains("Failed to find value for key")) {
                if (misValList.size() < 10000)
                    misValList.add(String.format("%s   [file: %s]", line, file.getName()));

                String patternString = ".*Failed to find value for key ([\\d]+)" +
                        "( and its reversed key ([\\d]+))? in cache ([-\\w]+).*";
                Pattern p = Pattern.compile(patternString);
                Matcher m = p.matcher(line);
                if (m.matches()) {
                    long key = Long.valueOf(m.group(1));
                    String cacheName = m.group(4);
                    long part = key % 1024;

                    if (!cachePartMap.keySet().contains(cacheName)) {
                        Set<Long> partSet = new TreeSet<>();
                        partSet.add(part);
                        cachePartMap.put(cacheName, partSet);
                    }
                    else
                        cachePartMap.get(cacheName).add(part);
                } else {
                    LOG.error("Failed to parse line: " + line);
                }
            }

            if (line.contains("Setting") && line.contains("flag to"))
                logSet.add(line);

            for (String useStr : summaryList) {
                if (line.contains(useStr))
                    logSet.add(line);
            }
        }
    }

    /**
     * @param str Source string.
     * @return {@code true} if source string contains any of excluded lines or {@code false} otherwise.
     */
    private boolean excluded(String str) {
        for (String excl : servExclList)
            if (str.contains(excl))
                return true;

        return false;
    }

    /**
     * @return List of server log files in main result directory.
     */
    private List<File> getServerLogList() {
        List<File> res = new ArrayList<>();

        Path logDir = Paths.get(mainDir.getAbsolutePath(), "servers");

        final File[] hostDirs = PocTesterUtils.getSubDirs(logDir, null);

        for (File hostDir : hostDirs) {
            File[] logs = hostDir.listFiles();

            for (File serverLog : logs) {
                if (serverLog.getName().endsWith(".log") || serverLog.getName().endsWith(".log.gz"))
                    res.add(serverLog);
            }
        }

        return res;
    }

    /**
     * @return List of client log files in main result directory.
     */
    private List<File> getTaskLogList() {
        List<File> res = new ArrayList<>();

        Path clientDir = Paths.get(mainDir.getAbsolutePath(), "clients");

        final File[] hostDirs = PocTesterUtils.getSubDirs(clientDir, null);

        for (File hostDir : hostDirs) {
            File[] taskDirs = PocTesterUtils.getSubDirs(hostDir.toPath(), "task-");

            for (File taskDir : taskDirs) {
                File[] taskLogs = taskDir.listFiles();

                for (File serverLog : taskLogs) {
                    if (serverLog.getName().endsWith(".log") || serverLog.getName().endsWith(".log.gz"))
                        res.add(serverLog);
                }
            }
        }

        return res;
    }

    /**
     * Decompresses files.
     *
     * @param compressedFile File to decompress.
     * @param decompressedFile Path to save decompressed file.
     */
    private void unzipFile(String compressedFile, String decompressedFile) {
        try {
            FileInputStream fileIn = new FileInputStream(compressedFile);

            GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);

            FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);

            int bytes_read;

            byte[] buff = new byte[1024];

            while ((bytes_read = gZIPInputStream.read(buff)) > 0)
                fileOutputStream.write(buff, 0, bytes_read);

            gZIPInputStream.close();

            fileOutputStream.close();
        }
        catch (IOException e) {
            LOG.error(String.format("Failed to decompress file '%s'", compressedFile), e);
        }
    }

    /**
     *
     */
    private static class ParsedLog extends HashMap<String, List<String>> {
        /**
         * Constructor.
         *
         * @param key Key.
         * @param val List of related lines.
         */
        private ParsedLog(String key, List<String> val) {
            if (key != null)
                put(key, val);
        }
    }
}
