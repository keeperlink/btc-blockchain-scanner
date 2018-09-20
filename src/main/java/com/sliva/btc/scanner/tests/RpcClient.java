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
package com.sliva.btc.scanner.tests;

//import com.google.gson.Gson;
import java.io.FileWriter;
import java.io.Writer;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.io.IOUtils;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Block;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.BlockChainInfo;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Unspent;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RpcClient {

    private static final BitcoinJSONRPCClient rpcClient;

    static {
        try {
            rpcClient = new BitcoinJSONRPCClient("http://localhost:17955");
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        auth();
        log.info("WalletInfo: " + rpcClient.getWalletInfo());
        log.info("List Accounts: " + rpcClient.listAccounts());
        log.info("List ReceivedByAddress: " + rpcClient.listReceivedByAddress());
        log.info("List SinceBlock: " + rpcClient.listSinceBlock().transactions());
        log.info("List Transactions: " + rpcClient.listTransactions());
        log.info("List Unspent: " + Arrays.deepToString(rpcClient.listUnspent().toArray()));
        printUnspentList(rpcClient.listUnspent());
        BlockChainInfo bci = rpcClient.getBlockChainInfo();
        log.info("BlockChainInfo: " + bci);
        Block block = rpcClient.getBlock(bci.blocks() - 2);
//        try (Writer w = new FileWriter("block-" + (bci.blocks() - 1) + ".json")) {
//            IOUtils.write(new Gson().toJson(block), w);
//        }
//        String txId1 = block.tx().get(1);
//        RawTransaction tx1 = rpcClient.getRawTransaction(txId1);
//        log.info("tx1: " + tx1);
//        log.info("tx1.json: " + new Gson().toJson(tx1));
//        try (Writer w = new FileWriter("tx-" + tx1.txId() + ".json")) {
//            IOUtils.write(new Gson().toJson(tx1), w);
//        }
//        log.info("block#" + (bci.blocks() - 1) + ": " + block);
//        log.info("Block.nTXNs=" + block.tx().size());
//        readBlocks(200239,500);
    }

    private static void printUnspentList(List<Unspent> list) {
        for (Unspent u : list) {
            log.info("\tUnspent: acc:" + u.account() + ", addr:" + u.address() + ", am:" + u.amount());
        }
    }

    private static void readBlocks(int start, int number) {
        for (int i = start; i < start + number; i++) {
            Block block = rpcClient.getBlock(i);
//        log.info("block#" + i + ": " + block);
            log.info("Block#" + i + ".nTXNs=" + block.tx().size());
        }
    }

    private static void auth() {
        final String rpcuser = "cer6Yxq2apNc";
        final String rpcpassword = "hY7cZp01WmCbvqPiyaLY67x";

        Authenticator.setDefault(new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(rpcuser, rpcpassword.toCharArray());
            }
        });
    }
}
