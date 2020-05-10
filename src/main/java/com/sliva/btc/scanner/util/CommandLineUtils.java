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

import static com.google.common.base.Preconditions.checkArgument;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

/**
 *
 * @author Sliva Co
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CommandLineUtils {

    @NonNull
    public static CmdArguments buildCmdArguments(String[] args, String command, CmdOptions optionsList) throws ParseException {
        CmdArguments cmd = new CmdArguments(args, command, optionsList);
        optionsList.applyArgumentsMethods.forEach(m -> invokeStaticMethod(m, cmd));
        return cmd;
    }

    @NonNull
    public static CommandLine parseCommandLine(String[] args, String command, CmdOptions optionsList)
            throws ParseException {
        CommandLineParser parser = new DefaultParser();
        Options opts = prepOptions(optionsList);
        CommandLine cmd = parser.parse(opts, args);
        if (cmd.hasOption("help")) {
            printHelpAndExit(command, opts);
        }
        return cmd;
    }

    @NonNull
    public static CmdOption buildOption(CmdOptions optionsList, String opt, String longOpt, boolean hasArg, String description) {
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

    @NonNull
    private static Options prepOptions(CmdOptions optionsList) {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        optionsList.optionsList.forEach(c -> options.addOption(c.getOpt(), c.getLongOpt(), c.isHasArg(), c.getDescription()));
        return options;
    }

    @SneakyThrows({IllegalAccessException.class, InvocationTargetException.class})
    private static void invokeStaticMethod(Method m, Object... args) {
        m.invoke(null, args);
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

    public static class CmdOptions {

        private final Collection<CmdOption> optionsList = new ArrayList<>();
        private final Collection<Method> applyArgumentsMethods = new ArrayList<>();

        @NonNull
        private CmdOptions add(CmdOption cmdOption) {
            optionsList.add(cmdOption);
            return this;
        }

        @NonNull
        @SneakyThrows(IllegalAccessException.class)
        public CmdOptions add(Class<?> classWithOptions) {
            Method m = MethodUtils.getAccessibleMethod(classWithOptions, "applyArguments", CmdArguments.class);
            checkArgument(m != null, "Class %s does not expose static method applyArguments(CmdArguments)", classWithOptions);
            applyArgumentsMethods.add(m);
            Field f = FieldUtils.getDeclaredField(classWithOptions, "CMD_OPTS", true);
            checkArgument(f != null, "Class %s does not expose static field CMD_OPTS", classWithOptions);
            checkArgument(f.get(null) instanceof CmdOptions, "Static CMD_OPTS field in class %s should contain non null instance of of CmdOptions", classWithOptions);
            CmdOptions cmdOptsValue = (CmdOptions) f.get(null);
            optionsList.addAll(cmdOptsValue.optionsList);
            return this;
        }

        @Deprecated
        public void forEach(Consumer<CmdOption> action) {
            optionsList.forEach(action);
        }
    }

    public static class CmdArguments {

        private final CommandLine commandLine;

        private CmdArguments(String[] args, String command, CmdOptions optionsList) throws ParseException {
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
