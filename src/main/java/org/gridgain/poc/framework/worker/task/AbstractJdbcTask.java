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

package org.gridgain.poc.framework.worker.task;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.poc.framework.worker.task.jdbc.QueryGenerator;
import org.gridgain.poc.framework.utils.PocTesterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import static org.gridgain.poc.framework.utils.PocTesterUtils.sleep;

/**
 * Common parent class for SQL testing tasks.
 */
public abstract class AbstractJdbcTask extends AbstractTask {
    /** */
    private static final Logger LOG = LogManager.getLogger(AbstractJdbcTask.class.getName());

    /** */
    protected static final String DFLT_GEN_PKG = "org.gridgain.poc.framework.worker.task.jdbc";

    protected static final String DFLT_JDBC_DATA_RANGE = "1:10";

    /** */
    protected String genClsPkg;

    /** */
    protected String genClsName;

    /** */
    protected QueryGenerator qryGen;

    /** */
    private static final String DEFAULT_CONNECTION_STRING = "jdbc:ignite:thin://jdbc_host_port/";

    private static final String MSG_TABLE = "msgTable";

    /** */
    private static final int DEFAULT_LIMIT = 50;

    /** */
    protected static AtomicBoolean created = new AtomicBoolean(false);

    /** */
    protected BufferedWriter reportWriter;

    protected ThreadLocal<String> host = new ThreadLocal<>();

    private List<String> availableServerHosts = new ArrayList<>();

    /**
     * Force JDBC Thin Driver init.
     */
    static {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /** Optional. */
    @Nullable
    protected String connString;

    /** Each connection is also a transaction, so we better pin them to threads. */
    private ThreadLocal<Connection> conn;

    /** */
    protected int limit;

    /**
     *
     * @param args
     */
    public AbstractJdbcTask(PocTesterArguments args) {
        super(args);
    }

    /**
     *
     * @param args Arguments.
     * @param props Task properties.
     */
    public AbstractJdbcTask(PocTesterArguments args, TaskProperties props){
        super(args, props);
    }

    /** {@inheritDoc} */
    @Override public void init() throws IOException {
        super.init();

        connString = props.getString("connectionString", DEFAULT_CONNECTION_STRING);
        limit = props.getInteger("limit", DEFAULT_LIMIT);
        genClsPkg = props.getString("genClassPackage", DFLT_GEN_PKG);
        genClsName = props.getString("genClassName");

        dataRange = props.getString("jdbcDataRange", DFLT_JDBC_DATA_RANGE);
        dataRangeFrom = parseRangeLong(dataRange)[0];
        dataRangeTo = parseRangeLong(dataRange)[1];
    }

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        if(genClsName != null) {
            String genClsNameFull = String.format("%s.%s", genClsPkg, genClsName);

            Class<?> cls = Class.forName(genClsNameFull);

            if (!QueryGenerator.class.isAssignableFrom(cls))
                throw new IllegalArgumentException();

            qryGen = (QueryGenerator)cls.getDeclaredConstructor().newInstance();
        }

        createReportFile();

        conn = initConnection();

        waitForActivation();

    }

    /**
     *
     * @param name
     * @return
     */
    protected boolean checkTable(String name){
        String qry = String.format("SELECT * FROM %s LIMIT 1", name);

        try (PreparedStatement stmt = getConnection().prepareStatement(qry)) {
            ResultSet rs = stmt.executeQuery();

            return rs.next();
        }
        catch (SQLException e) {
            LOG.error("Failed to execute query: " + qry, e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        super.tearDown();

        if (conn != null && conn.get() != null)
            U.closeQuiet(conn.get());

        conn = null;

        try {
            if (reportWriter != null)
                reportWriter.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    private void waitForActivation(){
        String qry = "DROP TABLE if exists TEMPTABLE;";

        boolean active = false;

        int cnt = 0;

        while(!active && cnt++ < 240) {
            try {
                Connection conn = getConnection();
                try (PreparedStatement stmt = conn.prepareStatement(qry)) {
                    stmt.execute();
                }
                active = true;
            }
            catch (SQLException e) {
                LOG.error("Failed to execute query: " + qry, e);
                sleep(2000L);
            }
            catch (Exception e) {
                LOG.error("Failed to execute query: " + qry, e);
                sleep(5000L);

            }
        }
    }

    protected ThreadLocal<Connection> initConnection(){
        return new ThreadLocal<Connection>() {
            @Override protected Connection initialValue() {
                try {
                    return setupConnection();
                }
                catch (SQLException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }

    /** {@inheritDoc} */
    protected boolean disableClient() {
        return true;
    }

    /**
     * @return JDBC conn.
     * @throws SQLException
     */
    protected Connection getConnection() throws SQLException {
        if (conn == null)
            conn = initConnection();

        if (!conn.get().isValid(10))
            conn.set(setupConnection());

        return conn.get();
    }

    /**
     * @return JDBC connection object
     * @throws SQLException
     */
    protected Connection setupConnection() throws SQLException {
        int attempt = 0;
        int max_attempts = 15;

        while (attempt++ < max_attempts) {
            try {
                return tryToConnect();
            } catch (SQLException e) {
                LOG.error(String.format("Cannot establish JDBC connection. Try %d out of %d", attempt, max_attempts));
                LOG.error(e.getMessage(), e);

                String sqlState = e.getSQLState();

                LOG.warn("SQLState: {}", sqlState);

                if (sqlState.equals(SqlStateCode.CLIENT_CONNECTION_FAILED)
                        | sqlState.equals(SqlStateCode.CONNECTION_CLOSED)
                        | sqlState.equals(SqlStateCode.CONNECTION_FAILURE)
                        | sqlState.equals(SqlStateCode.CONNECTION_REJECTED)
                ) {
                    // Remove current connection host from the hosts pool
                    LOG.info("Removing host {} from the hosts pool", host.get());
                    availableServerHosts.remove(host.get());
                }

                sleep(1000);

                if (attempt >= max_attempts) {
                    LOG.error("Max attempts reached. Will not retry");
                    throw e;
                } else {
                    LOG.info("Will retry");
                }
            }
        }

        return null;
    }

    /** */
    protected abstract Logger log();

    /**
     *
     */
    private void createReportFile() {
        Path reportDir = Paths.get(homeDir, "log", clientDirName, "reports");

        String consID = System.getProperty("CONSISTENT_ID");

        Path report = Paths.get(homeDir, "log", clientDirName, "reports", String.format("report-%s.log", consID));

        try {
            if (!reportDir.toFile().exists())
                Files.createDirectories(reportDir);

            reportWriter = Files.newBufferedWriter(report, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }

        catch (IOException e) {
            log().error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    /**
     *
     * @param qrs List of queries.
     */
    protected void printDebugListQuery(List<String> qrs){
        LOG.debug(String.format("list size = %d", qrs.size()));

        for(String qry : qrs)
            LOG.debug("Query = " + qry);
    }

    private String getConnString() {
        if (connString.contains("jdbc_host_port")) {
            host.set(getRandomAvailableServerHost());
            return (connString.replace("jdbc_host_port", host.get()));
        }
        return connString;
    }

    /**
     *
     * @return JDBC connection object
     */
    private Connection tryToConnect() throws SQLException {
        String conn_str = getConnString();
        LOG.debug("Connecting to " + conn_str);
        return DriverManager.getConnection(conn_str);
    }

    @Override protected Integer getMsgFlag(String key) {
        Integer val = null;

        try (Connection conn = getConnection()) {
            val = getJdbcFlag(conn, key);
        }
        catch (SQLException e) {
            LOG.error("Failed to get flag {}", key);
            tearDown();
        }

        return val;
    }

    @Override protected void setMsgFlag(String key, Integer val) {
        try (Connection conn = getConnection()) {
            setJdbcFlag(conn, key, val);
        } catch (SQLException e) {
            LOG.error("Failed to set flag {} to {}", key, val);
            tearDown();
        }
    }

    @Override protected Integer incMsgFlag(String key) {
        LOG.info("Incrementing flag {}", key);

        Integer val = null;

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            val = getJdbcFlag(conn, key);

            if (val == null)
                val = 0;

            setJdbcFlag(conn, key, val + 1);

            conn.commit();
        }
        catch (SQLException e) {
            LOG.error("Failed to increase flag {}", key);
            tearDown();
        }

        return val;
    }

    @Override protected Integer decMsgFlag(String key) {
        LOG.info("Decrementing flag {}", key);

        Integer val = null;

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            val = getJdbcFlag(conn, key);

            if (val == null)
                val = 1; // Decrease will reset the value to zero

            setJdbcFlag(conn, key, val - 1);

            conn.commit();
        }
        catch (SQLException e) {
            LOG.error("Failed to decrease flag {}", key);
            tearDown();
        }

        return val;
    }

    private Integer getJdbcFlag(Connection conn, String flag) throws SQLException {
        int attempt = 0;
        int max_attempts = 10;

        String qry = String.format("SELECT val FROM %s WHERE flag='%s';", MSG_TABLE, flag);
        Integer val = null;

        while (attempt++ < max_attempts) {
            try {
                try (
                    PreparedStatement stmt = conn.prepareStatement(qry);
                    ResultSet rs = stmt.executeQuery()
                ) {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colNum = rsmd.getColumnCount();

                    while (rs.next()) {
                        for (int j = 1; j <= colNum; j++) {
                            String colName = rsmd.getColumnName(j);
                            if (colName.equalsIgnoreCase("VAL"))
                                val = rs.getInt(j);
                        }
                    }
                }
                break;
            } catch (SQLException e) {
                LOG.error("Failed to execute statement: {}", qry);
                LOG.error(e);

                if (attempt >= max_attempts) {
                    LOG.error("Max attempts reached. Will not retry");
                    throw e;
                }
                else {
                    LOG.info("Will retry");
                    conn = getConnection();
                }
            }
        }

        return val;
    }

    private void setJdbcFlag(Connection conn, String flag, Integer val) throws SQLException {
        int attempt = 0;
        int max_attempts = 10;

        String qry = String.format("MERGE INTO %s (flag, val) values ('%s', '%s');", MSG_TABLE, flag, val);

        while (attempt++ < max_attempts) {
            try {
                try (PreparedStatement stmt = conn.prepareStatement(qry)) {
                    stmt.executeUpdate();
                }

                return;
            }
            catch (SQLException e) {
                LOG.error("Failed to execute query: " + qry, e);
                if (attempt >= max_attempts) {
                    throw e;
                } else {
                    LOG.error("Will retry");
                    conn = getConnection();
                }
            }
        }
    }

    protected String getRandomAvailableServerHost() {
        if (availableServerHosts.isEmpty()) {
            List<String> servHosts = PocTesterUtils.getHostList(args.getServerHosts(), true);
            availableServerHosts = Collections.synchronizedList(servHosts);
        }

        assert !availableServerHosts.isEmpty();

        Collections.shuffle(availableServerHosts);

        return availableServerHosts.get(0);
    }
}
