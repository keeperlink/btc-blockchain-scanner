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

import com.sliva.btc.scanner.util.Utils;
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

    public Block getBlock(int height) {
        long s = System.currentTimeMillis();
        try {
            return getClient().getBlock(height);
        } finally {
            log.trace("BtcClient.getBlock(" + height + "): Runtime=" + (System.currentTimeMillis() - s) + " ms.");
        }
    }

    public Block getBlock(String hash) {
        long s = System.currentTimeMillis();
        try {
            return getClient().getBlock(hash);
        } finally {
            log.trace("BtcClient.getBlock(" + hash + "): Runtime=" + (System.currentTimeMillis() - s) + " ms.");
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

    public static void applyArguments(CommandLine cmd) {
        Properties prop = Utils.loadProperties(cmd.getOptionValue("rpc-config"));
        RPC_URL = cmd.getOptionValue("rpc-url", prop.getProperty("rpc-url", RPC_URL));
        RPC_USER = cmd.getOptionValue("rpc-user", prop.getProperty("rpc-user", RPC_USER));
        RPC_PASSWORD = cmd.getOptionValue("rpc-password", prop.getProperty("rpc-password", RPC_PASSWORD));
    }

    public static Options addOptions(Options options) {
        options.addOption("r", "rpc-url", true, "RPC URL to running bitcoin core. Default is '" + RpcClient.RPC_URL + "'.");
        options.addOption("x", "rpc-user", true, "RPC user name.");
        options.addOption("y", "rpc-password", true, "RPC password.");
        options.addOption(null, "rpc-config", true, "Configuration file name with RPC url, user and password values.");
        return options;
    }
}
