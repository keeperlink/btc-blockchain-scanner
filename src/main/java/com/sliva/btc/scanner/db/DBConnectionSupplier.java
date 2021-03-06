/*
 * Copyright 2018 Sliva Co.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sliva.btc.scanner.db;

import com.sliva.btc.scanner.db.utils.DBMetaData;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOption;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOptions;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.LazyInitializer;
import com.sliva.btc.scanner.util.Utils;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.sql.DataSource;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Database connection supplier. The class creates and keeps open connections
 * one per each thread.
 *
 * #Thread-safe
 *
 * @author Sliva Co
 */
@Slf4j
public class DBConnectionSupplier implements Supplier<Connection>, DataSource, AutoCloseable {

    private static String DEFAULT_CONN_URL = "jdbc:mysql://localhost:3306/btc_default_db"
            + "?verifyServerCertificate=false&useSSL=true"
            + "&useUnicode=true"
            + "&characterEncoding=UTF-8"
            + "&rewriteBatchedStatements=true"
            + "&defaultRowPrefetch=10000";
    private static String DEFAULT_DB_USER = "root";
    private static String DEFAULT_DB_PASSWORD = "password";

    public static final CmdOptions CMD_OPTS = new CmdOptions();
    public static final CmdOption dbUrlOpt = buildOption(CMD_OPTS, null, "db-url", true, "DB URL, i.e. 'jdbc:mysql://localhost:3306/'.");
    public static final CmdOption dbUserOpt = buildOption(CMD_OPTS, null, "db-user", true, "DB user name.");
    public static final CmdOption dbPasswordOpt = buildOption(CMD_OPTS, null, "db-password", true, "DB password.");
    public static final CmdOption dbConfigOpt = buildOption(CMD_OPTS, null, "db-config", true, "Configuration file name with db url, user and password values.");

    private final ThreadLocal<Connection> conn;
    private final LazyInitializer<String> dbname;
    private final LazyInitializer<DBMetaData> dbMetaData;

    public static void applyArguments(CmdArguments cmdArguments) {
        checkArgument(cmdArguments != null, "Argument 'cmdArguments' is null");
        Properties prop = Utils.loadProperties(cmdArguments.getOption(dbConfigOpt).orElse(null));
        DEFAULT_CONN_URL = cmdArguments.getOption(dbUrlOpt).orElseGet(() -> prop.getProperty(dbUrlOpt.getLongOpt(), DEFAULT_CONN_URL));
        DEFAULT_DB_USER = cmdArguments.getOption(dbUserOpt).orElseGet(() -> prop.getProperty(dbUserOpt.getLongOpt(), DEFAULT_DB_USER));
        DEFAULT_DB_PASSWORD = cmdArguments.getOption(dbPasswordOpt).orElseGet(() -> prop.getProperty(dbPasswordOpt.getLongOpt(), DEFAULT_DB_PASSWORD));
    }

    public DBConnectionSupplier() {
        this(DEFAULT_CONN_URL, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
    }

    public DBConnectionSupplier(String dbName) {
        this(DEFAULT_CONN_URL.replaceAll("btc_default_db", dbName), DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
    }

    public DBConnectionSupplier(String url, String user, String password) {
        this.conn = ThreadLocal.withInitial(() -> makeJDBCConnection(url, user, password));
        this.dbname = new LazyInitializer<>(this::_getCatalog);
        this.dbMetaData = new LazyInitializer<>(this::_getDBMetaData);
    }

    /**
     * Get current database name
     *
     * @return current database name
     */
    @NonNull
    public String getDBName() {
        return dbname.get();
    }

    /**
     * Get current DB meta data.
     *
     * @return DBMetaData instance for current DB.
     */
    @NonNull
    public DBMetaData getDBMetaData() {
        return dbMetaData.get();
    }

    /**
     * Check that all tables exist in current database and throw
     * IlligalStateException if any is missing.
     *
     * @param tableNames table names to check
     * @return this
     */
    @NonNull
    public DBConnectionSupplier checkTablesExist(String... tableNames) {
        Stream.of(tableNames).forEach(tableName -> checkState(getDBMetaData().hasTable(tableName), "Table \"%s\" does not exist in database \"%s\"", tableName, getDBName()));
        return this;
    }

    /**
     * Get open database connection local to current thread.
     *
     * @return
     */
    @NonNull
    @Override
    public Connection get() {
        return conn.get();
    }

    /**
     * Close current thread's connection.
     */
    @SneakyThrows(SQLException.class)
    @Override
    public void close() {
        get().close();
        conn.remove();
    }

    /**
     * Prepare SQL statement.
     *
     * @param query Query string
     * @param requiredIndexes list of fields that required to be indexed in
     * order to execute this query. If any of these indexes is missing in DB,
     * then returned instance will fail on execution request.
     * @return DBPreparedStatement instance
     */
    @NonNull
    public DBPreparedStatement prepareStatement(String query, String... requiredIndexes) {
        checkArgument(query != null, "Argument 'query' is null");
        String reason = Stream.of(requiredIndexes).filter(idxField -> !getDBMetaData().isIndexed(idxField)).map(r -> "Missing index on field \"" + r + '"').findFirst().orElse(null);
        return new DBPreparedStatement(query, this, reason);
    }

    /**
     * Prepare SQL statement that cannot be executed.
     *
     * @param query Query string
     * @param reason reason why this statement cannot be executed.
     * @return DBPreparedStatement instance
     */
    @NonNull
    public DBPreparedStatement prepareNonExecutableStatement(String query, String reason) {
        checkArgument(query != null, "Argument 'query' is null");
        checkArgument(reason != null, "Argument 'reason' is null");
        return new DBPreparedStatement(query, this, reason);
    }

    @Deprecated
    public static void applyArguments(CommandLine cmd) {
        Properties prop = Utils.loadProperties(cmd.getOptionValue(dbConfigOpt.getLongOpt()));
        DEFAULT_CONN_URL = cmd.getOptionValue(dbUrlOpt.getLongOpt(), prop.getProperty(dbUrlOpt.getLongOpt(), DEFAULT_CONN_URL));
        DEFAULT_DB_USER = cmd.getOptionValue(dbUserOpt.getLongOpt(), prop.getProperty(dbUserOpt.getLongOpt(), DEFAULT_DB_USER));
        DEFAULT_DB_PASSWORD = cmd.getOptionValue(dbPasswordOpt.getLongOpt(), prop.getProperty(dbPasswordOpt.getLongOpt(), DEFAULT_DB_PASSWORD));
    }

    @Deprecated
    public static Options addOptions(Options options) {
        CMD_OPTS.forEach(o -> options.addOption(o.getOpt(), o.getLongOpt(), o.isHasArg(), o.getDescription()));
        return options;
    }

    @NonNull
    @Override
    public Connection getConnection() throws SQLException {
        return get();
    }

    @NonNull
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return get();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @SneakyThrows(SQLException.class)
    private String _getCatalog() {
        return get().getCatalog();
    }

    private DBMetaData _getDBMetaData() {
        return new DBMetaData(this);
    }

    private Connection makeJDBCConnection(String url, String user, String password) {
        try {
            Connection con = DriverManager.getConnection(url, user, password);
            con.createStatement().execute("SET sql_log_bin=OFF");
            return con;
        } catch (SQLException e) {
            //log.error("url=" + url, e);
            throw new IllegalStateException(e);
        }
    }
}
