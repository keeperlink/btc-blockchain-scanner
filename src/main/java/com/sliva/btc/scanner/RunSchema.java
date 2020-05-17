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
package com.sliva.btc.scanner;

import com.google.common.io.Resources;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildCmdArguments;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

/**
 *
 * @author whost
 */
@Slf4j
public class RunSchema {

    private static final String SCHEMA_LOCATION = "schema";

    private static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions().add(DBConnectionSupplier.class);
    private static final CommandLineUtils.CmdOption createOpt = buildOption(CMD_OPTS, null, "create", true, "Create DB schema");
    private static final CommandLineUtils.CmdOption truncateAllTablesOpt = buildOption(CMD_OPTS, null, "truncate-all-tables", false, "Truncate all tables in schema");
    private static final CommandLineUtils.CmdOption dropAllTablesOpt = buildOption(CMD_OPTS, null, "drop-all-tables", false, "Drop all tables in schema");

    private final DBConnectionSupplier con = new DBConnectionSupplier();

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        CmdArguments cmd = buildCmdArguments(args, Main.Command.schema.name(), "Operations with DB schema", null, CMD_OPTS);
        RunSchema r = new RunSchema();
        if (cmd.hasOption(createOpt)) {
            r.createSchema(cmd.getOption(createOpt).get());
        } else if (cmd.hasOption(truncateAllTablesOpt)) {
            r.truncateAllTables();
        } else if (cmd.hasOption(dropAllTablesOpt)) {
            r.dropAllTables();
        }
    }

    @SuppressWarnings("UseSpecificCatch")
    public RunSchema() {
    }

    private void createSchema(String createSchemaName) throws Exception {
        URL url;
        try {
            url = Resources.getResource(SCHEMA_LOCATION + '/' + createSchemaName + ".sql");
        } catch (IllegalArgumentException ex) {
            log.error("Cannot find resource \"" + createSchemaName + "\"", ex);
            log.info("Available options: {}", new Reflections(SCHEMA_LOCATION, new ResourcesScanner())
                    .getResources(Pattern.compile(".*\\.sql")).stream().filter(s -> s.endsWith(".sql")).map(s -> '"' + s.replace(SCHEMA_LOCATION + '/', "").replace(".sql", "\"")).collect(Collectors.joining(", ")));

            return;
        }
        String query = Resources.toString(url, StandardCharsets.UTF_8);
        Stream.of(query.split(";")).filter(StringUtils::isNotBlank).forEach(this::runQuery);
    }

    private void truncateAllTables() throws Exception {
        con.getDBMetaData().getTables().keySet().stream().peek(t -> log.info("Truncating table {}", t)).map("TRUNCATE TABLE "::concat).forEach(this::runQuery);
    }

    private void dropAllTables() throws Exception {
        con.getDBMetaData().getTables().keySet().stream().peek(t -> log.info("Dropping table {}", t)).map("DROP TABLE "::concat).forEach(this::runQuery);
    }

    @SneakyThrows(SQLException.class)
    private void runQuery(String query) {
        con.getConnection().createStatement().execute(query);
    }
}
