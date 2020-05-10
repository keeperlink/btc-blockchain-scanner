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

import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOption;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOptions;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;
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
public class DBConnectionSupplier implements Supplier<Connection> {

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

    private final String dbname;
    private final ThreadLocal<Connection> conn;

    public DBConnectionSupplier() {
        this(DEFAULT_CONN_URL, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
    }

    public DBConnectionSupplier(String dbName) {
        this(DEFAULT_CONN_URL.replaceAll("btc_default_db", dbName), DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
    }

    public DBConnectionSupplier(String url, String user, String password) {
        this.dbname = url;
        this.conn = ThreadLocal.withInitial(() -> makeJDBCConnection(url, user, password));
    }

    public String getDbname() {
        return dbname;
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

    /**
     * Get open database connection local to current thread.
     *
     * @return
     */
    @Override
    public Connection get() {
        return conn.get();
    }

    @SneakyThrows(SQLException.class)
    public void close() {
        get().close();
        conn.remove();
    }

    public DBPreparedStatement prepareStatement(String query) {
        return new DBPreparedStatement(query, this);
    }

    public static void applyArguments(CmdArguments cmdArguments) {
        Properties prop = Utils.loadProperties(cmdArguments.getOption(dbConfigOpt).orElse(null));
        DEFAULT_CONN_URL = cmdArguments.getOption(dbUrlOpt).orElseGet(() -> prop.getProperty(dbUrlOpt.getLongOpt(), DEFAULT_CONN_URL));
        DEFAULT_DB_USER = cmdArguments.getOption(dbUserOpt).orElseGet(() -> prop.getProperty(dbUserOpt.getLongOpt(), DEFAULT_DB_USER));
        DEFAULT_DB_PASSWORD = cmdArguments.getOption(dbPasswordOpt).orElseGet(() -> prop.getProperty(dbPasswordOpt.getLongOpt(), DEFAULT_DB_PASSWORD));
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
}
