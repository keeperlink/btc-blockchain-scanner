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
package com.sliva.btc.scanner.neo4j;

import com.sliva.btc.scanner.util.Utils;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

/**
 *
 * @author whost
 */
public class NeoConnection implements AutoCloseable {

    private static String DEFAULT_URL = "bolt://localhost:7687";
    private static String DEFAULT_USER = "neo4j";
    private static String DEFAULT_PASSWORD = "password";
    private final Driver driver;

    public NeoConnection() {
        this(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD);
    }

    public NeoConnection(String url, String user, String password) {
        this.driver = getDriver(url, user, password);
    }

    public Driver getDriver() {
        return driver;
    }

    public Session getSession() {
        return getDriver().session();
    }

    private static Driver getDriver(String url, String user, String password) {
        Config.ConfigBuilder configBuilder = Config.build();
        configBuilder.withLeakedSessionsLogging();
        //configBuilder.withMaxConnectionPoolSize(10);
        //configBuilder.withLogging(new ConsoleLogging(Level.FINEST));
        configBuilder.withoutEncryption();
        return GraphDatabase.driver(url, AuthTokens.basic(user, password), configBuilder.toConfig());
    }

    public static void applyArguments(CommandLine cmd) {
        Properties prop = Utils.loadProperties(cmd.getOptionValue("neo-config"));
        DEFAULT_URL = cmd.getOptionValue("neo-url", prop.getProperty("neo-url", DEFAULT_URL));
        DEFAULT_USER = cmd.getOptionValue("neo-user", prop.getProperty("neo-user", DEFAULT_USER));
        DEFAULT_PASSWORD = cmd.getOptionValue("neo-password", prop.getProperty("neo-password", DEFAULT_PASSWORD));
    }

    public static Options addOptions(Options options) {
        options.addOption(null, "neo-url", true, "Neo4j URL. Default: " + DEFAULT_URL);
        options.addOption(null, "neo-user", true, "Neo4j user name.");
        options.addOption(null, "neo-password", true, "Neo4j password.");
        options.addOption(null, "neo-config", true, "Configuration file name with Neo4j url, user and password values.");
        return options;
    }

    @Override
    public void close() {
        driver.close();
    }
}
