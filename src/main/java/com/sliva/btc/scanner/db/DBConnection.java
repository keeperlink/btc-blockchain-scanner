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

import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DBConnection {

    public static String DEFAULT_CONN_URL = "jdbc:mysql://localhost:3306/btc3"
            + "?verifyServerCertificate=false&useSSL=true"
            + "&useUnicode=true"
            + "&characterEncoding=UTF-8"
            + "&rewriteBatchedStatements=true"
            + "&defaultRowPrefetch=10000";
    private static String DEFAULT_DB_USER = "root";
    private static String DEFAULT_DB_PASSWORD = "password";

    public static final Collection<CommandLineUtils.CmdOption> CMD_OPTS = new ArrayList<>();
    public static final CommandLineUtils.CmdOption dbUrlOpt = buildOption(CMD_OPTS, null, "db-url", true, "DB URL, i.e. 'jdbc:mysql://localhost:3306/'.");
    public static final CommandLineUtils.CmdOption dbUserOpt = buildOption(CMD_OPTS, null, "db-user", true, "DB user name.");
    public static final CommandLineUtils.CmdOption dbPasswordOpt = buildOption(CMD_OPTS, null, "db-password", true, "DB password.");
    public static final CommandLineUtils.CmdOption dbConfigOpt = buildOption(CMD_OPTS, null, "db-config", true, "Configuration file name with db url, user and password values.");

    private final String dbname;
    private final ThreadLocal<Connection> conn;

    public DBConnection() {
        this(DEFAULT_CONN_URL, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
    }

    public DBConnection(String dbName) {
        this(DEFAULT_CONN_URL.replaceAll("btc3", dbName), DEFAULT_DB_USER, DEFAULT_DB_PASSWORD);
    }

    public DBConnection(String url, String user, String password) {
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

    public Connection getConnection() {
        return conn.get();
    }

    public ThreadLocal<PreparedStatement> prepareStatement(final String sql) {
        return ThreadLocal.withInitial(() -> {
            try {
                return getConnection().prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            } catch (SQLException e) {
                throw new IllegalStateException(sql, e);
            }
        });
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
