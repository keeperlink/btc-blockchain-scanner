/*
 * Copyright 2020 Sliva Co.
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
package com.sliva.btc.scanner.util;

import java.util.Collection;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author Sliva Co
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CommandLineUtils {

    public static CmdArguments buildCmdArguments(String[] args, String command, Collection<CmdOption> optionsList) throws ParseException {
        return new CmdArguments(args, command, optionsList);
    }

    public static CommandLine parseCommandLine(String[] args, String command, Collection<CmdOption> optionsList)
            throws ParseException {
        CommandLineParser parser = new DefaultParser();
        Options opts = prepOptions(optionsList);
        CommandLine cmd = parser.parse(opts, args);
        if (cmd.hasOption("help")) {
            printHelpAndExit(command, opts);
        }
        return cmd;
    }

    public static CmdOption buildOption(Collection<CmdOption> optionsList, String opt, String longOpt, boolean hasArg, String description) {
        CmdOption cmdOption = new CmdOption(opt, longOpt, hasArg, description);
        optionsList.add(cmdOption);
        return cmdOption;
    }

    public static void printHelpAndExit(String command, Options opts) {
        System.out.println("Available options:");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java <jar> " + command + " [options]", opts);
        System.exit(1);
    }

    private static Options prepOptions(Collection<CmdOption> optionsList) {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        optionsList.forEach(c -> options.addOption(c.getOpt(), c.getLongOpt(), c.isHasArg(), c.getDescription()));
        return options;
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    public static class CmdOption {

        private final String opt;
        @NonNull
        private final String longOpt;
        private final boolean hasArg;
        @NonNull
        private final String description;
    }

    public static class CmdArguments {

        private final CommandLine commandLine;

        private CmdArguments(String[] args, String command, Collection<CmdOption> optionsList) throws ParseException {
            this.commandLine = parseCommandLine(args, command, optionsList);
        }

        public boolean hasOption(CmdOption opt) {
            return commandLine.hasOption(opt.getLongOpt());
        }

        @NonNull
        public Optional<String> getOption(CmdOption opt) {
            return Optional.ofNullable(commandLine.getOptionValue(opt.getLongOpt()));
        }
    }
}
