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
package com.sliva.btc.scanner.rpc;

import com.sliva.btc.scanner.util.CommandLineUtils;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Block;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;
import wf.bitcoin.javabitcoindrpcclient.GenericRpcException;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RpcClient {

    public static String RPC_URL = "http://localhost:17955";
    public static String RPC_USER = "user";
    public static String RPC_PASSWORD = "password";

    public static final Collection<CommandLineUtils.CmdOption> CMD_OPTS = new ArrayList<>();
    public static final CommandLineUtils.CmdOption rpcUrlOpt = buildOption(CMD_OPTS, "r", "rpc-url", true, "RPC URL to running bitcoin core. Default is '" + RpcClient.RPC_URL + "'.");
    public static final CommandLineUtils.CmdOption rpcUserOpt = buildOption(CMD_OPTS, "x", "rpc-user", true, "RPC user name.");
    public static final CommandLineUtils.CmdOption rpcPasswordOpt = buildOption(CMD_OPTS, "y", "rpc-password", true, "RPC password.");
    public static final CommandLineUtils.CmdOption rpcConfigOpt = buildOption(CMD_OPTS, null, "rpc-config", true, "Configuration file name with RPC url, user and password values.");

    private static final ThreadLocal<RpcClient> clientThreaded = ThreadLocal.withInitial(() -> new RpcClient());

    private final BitcoinJSONRPCClient client;

    @SuppressWarnings("CallToPrintStackTrace")
    public RpcClient() {
        client = new CustomBitcoinJSONRPCClient(RPC_URL, RPC_USER, RPC_PASSWORD);
    }

    public static RpcClient getInstance() {
        return clientThreaded.get();
    }

    public BitcoinJSONRPCClient getClient() {
        return client;
    }

    public String getBlockHash(int height) {
        long s = System.currentTimeMillis();
        try {
            return getClient().getBlockHash(height);
        } finally {
            log.trace("BtcClient.getBlockHash({}): Runtime={} ms.", height, System.currentTimeMillis() - s);
        }
    }

    public Block getBlock(int height) {
        long s = System.currentTimeMillis();
        try {
            return getClient().getBlock(height);
        } finally {
            log.trace("BtcClient.getBlock({}): Runtime={} ms.", height, System.currentTimeMillis() - s);
        }
    }

    public Block getBlock(String hash) {
        long s = System.currentTimeMillis();
        try {
            return getClient().getBlock(hash);
        } finally {
            log.trace("BtcClient.getBlock({}): Runtime={} ms.", hash, System.currentTimeMillis() - s);
        }
    }

    public String getRawBlock(int height) {
        long s = System.currentTimeMillis();
        try {
            return getRawBlock(getBlockHash(height));
        } finally {
            log.trace("BtcClient.getRawBlock({}): Runtime={} ms.", height, System.currentTimeMillis() - s);
        }
    }

    public String getRawBlock(String hash) {
        long s = System.currentTimeMillis();
        try {
            return getClient().getRawBlock(hash);
        } finally {
            log.trace("BtcClient.getRawBlock({}): Runtime={} ms.", hash, System.currentTimeMillis() - s);
        }
    }

    public RawTransaction getRawTransaction(String txId) throws GenericRpcException {
        long s = System.currentTimeMillis();
        try {
            return getClient().getRawTransaction(txId);
        } finally {
            long runtime = System.currentTimeMillis() - s;
            if (runtime > 3000) {
                log.debug("getRawTransaction({}): Slow processing. Runtime: {} sec.", txId, runtime / 1000);
            }
        }
    }

    public int getBlocksNumber() {
        BitcoindRpcClient.BlockChainInfo bci = getClient().getBlockChainInfo();
        return bci.blocks();
    }

    public static void applyArguments(CommandLineUtils.CmdArguments cmdArguments) {
        Properties prop = Utils.loadProperties(cmdArguments.getOption(rpcConfigOpt).orElse(null));
        RPC_URL = cmdArguments.getOption(rpcUrlOpt).orElseGet(() -> prop.getProperty(rpcUrlOpt.getLongOpt(), RPC_URL));
        RPC_USER = cmdArguments.getOption(rpcUserOpt).orElseGet(() -> prop.getProperty(rpcUserOpt.getLongOpt(), RPC_USER));
        RPC_PASSWORD = cmdArguments.getOption(rpcPasswordOpt).orElseGet(() -> prop.getProperty(rpcPasswordOpt.getLongOpt(), RPC_PASSWORD));
    }

    @Deprecated
    public static void applyArguments(CommandLine cmd) {
        Properties prop = Utils.loadProperties(cmd.getOptionValue(rpcConfigOpt.getLongOpt()));
        RPC_URL = cmd.getOptionValue(rpcUrlOpt.getLongOpt(), prop.getProperty(rpcUrlOpt.getLongOpt(), RPC_URL));
        RPC_USER = cmd.getOptionValue(rpcUserOpt.getLongOpt(), prop.getProperty(rpcUserOpt.getLongOpt(), RPC_USER));
        RPC_PASSWORD = cmd.getOptionValue(rpcPasswordOpt.getLongOpt(), prop.getProperty(rpcPasswordOpt.getLongOpt(), RPC_PASSWORD));
    }

    @Deprecated
    public static Options addOptions(Options options) {
        CMD_OPTS.forEach(o -> options.addOption(o.getOpt(), o.getLongOpt(), o.isHasArg(), o.getDescription()));
        return options;
    }
}
