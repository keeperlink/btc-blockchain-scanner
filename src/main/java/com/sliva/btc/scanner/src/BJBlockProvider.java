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
package com.sliva.btc.scanner.src;

import com.sliva.btc.scanner.rpc.RpcClient;
import com.sliva.btc.scanner.util.BJBlockHandler;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOption;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOptions;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import java.io.File;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 *
 * @author Sliva Co
 */
public class BJBlockProvider implements BlockProvider<BJBlock> {

    public static final CmdOptions CMD_OPTS = new CmdOptions();
    public static final CmdOption fullBlocksPathOpt = buildOption(CMD_OPTS, null, "full-blocks-path", true, "Path to pre-loaded full blocks. Reading from pre-loaded full blocks is much faster than calling Bitcoin Core RPC. Helpful for massive update");

    private final RpcClient client = new RpcClient();

    @Override
    public BJBlock getBlock(int height) {
        try {
            return new BJBlock(client.getBlock(height).hash(), height);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public BJBlock getBlock(String hash) {
        try {
            return new BJBlock(hash, client.getBlock(hash).height());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void applyArguments(CmdArguments cmdArguments) {
        cmdArguments.getOption(fullBlocksPathOpt).map(File::new).ifPresent(f -> BJBlockHandler.FULL_BLOCKS_PATH = f);
    }

    @Deprecated
    public static void applyArguments(CommandLine cmd) {
        BJBlockHandler.FULL_BLOCKS_PATH = new File(cmd.getOptionValue(fullBlocksPathOpt.getLongOpt(), BJBlockHandler.FULL_BLOCKS_PATH.getAbsolutePath()));
    }

    @Deprecated
    public static Options addOptions(Options options) {
        CMD_OPTS.forEach(o -> options.addOption(o.getOpt(), o.getLongOpt(), o.isHasArg(), o.getDescription()));
        return options;
    }
}
