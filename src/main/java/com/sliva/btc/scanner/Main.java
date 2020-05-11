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

import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildCmdArguments;
import java.util.Arrays;

/**
 *
 * @author Sliva Co
 */
public class Main {

    private static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions();

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    @SuppressWarnings("UnnecessaryReturnStatement")
    public static void main(String[] args) throws Exception {
        String[] args1 = Arrays.copyOfRange(args, 0, Math.min(1, args.length));
        CmdArguments cmdArguments = buildCmdArguments(args1, "<command>", "Operations on blockchain", "\nAvailable commsnds: " + Arrays.toString(Command.values()), CMD_OPTS);
        if (args.length == 0) {
            cmdArguments.printHelpAndExit();
        }
        try {
            Command cmd = Command.valueOf(args[0]);
            switch (cmd) {
                case update:
                    RunFullScan.main(removeCmd(args));
                    return;
                case update_spent:
                    RunUpdateSpent.main(removeCmd(args));
                    return;
                case update_wallets:
                    RunUpdateWallets2.main(removeCmd(args));
                    return;
                case prepare_blocks:
                    RunPrepareBlockFiles.main(removeCmd(args));
                    return;
                case address:
                    RunAddress.main(removeCmd(args));
                    return;
                case load_neo4j:
                    RunNeoLoader.main(removeCmd(args));
                    return;
                case update_neo_wallets:
                    RunNeoUpdateWallets.main(removeCmd(args));
                    return;
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Unknown command: " + args[0]);
            e.printStackTrace();
            cmdArguments.printHelpAndExit();
        }
    }

    private static String[] removeCmd(String[] args) {
        return Arrays.copyOfRange(args, 1, args.length);
    }

    public enum Command {
        update,
        update_spent,
        update_wallets,
        prepare_blocks,
        address,
        load_neo4j,
        update_neo_wallets
    }
}
