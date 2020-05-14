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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.fasade.DbQueryBlock;
import com.sliva.btc.scanner.db.fasade.DbQueryTransaction;
import com.sliva.btc.scanner.db.fasade.DbUpdateTransaction;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.rpc.ParallelGetBlock;
import com.sliva.btc.scanner.rpc.RpcClient;
import java.sql.SQLException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Block;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class UpdateTransactionId {

    private static final int RPC_THREADS = 70;
    private static BitcoinJSONRPCClient client;
    private static DBConnectionSupplier conn;
    private static DbQueryBlock queryBlock;
    private static DbQueryTransaction queryTransaction;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        log.debug("START");
        client = new RpcClient().getClient();
        conn = new DBConnectionSupplier("btc");
        queryBlock = new DbQueryBlock(conn);
        queryTransaction = new DbQueryTransaction(conn);
        BitcoindRpcClient.BlockChainInfo bci = client.getBlockChainInfo();
        log.info("BlockChainInfo: {}", bci);
        Optional<BtcTransaction> lastTx = queryTransaction.getLastTransaction();
        log.info("lastTx={}", lastTx);
        int firstBlock = lastTx.map(BtcTransaction::getBlockHeight).orElse(1);
        int numBlocks = queryBlock.findLastHeight().orElse(-1) - firstBlock + 1;
        log.info("firstBlock={}, numBlocks={}", firstBlock, numBlocks);
        updateDb(RPC_THREADS, firstBlock, numBlocks, lastTx.map(BtcTransaction::getTransactionId).orElse(0));
    }

//    private static int findFirstBlockToUpdate() throws SQLException {
//        int nBlocks = queryBlock.findLastHeight();
//        int minBlock = 0;
//        int maxBlock = nBlocks;
//        while (maxBlock - minBlock > 1) {
//            int middle = (minBlock + maxBlock) / 2;
//            if (isUpdated(middle)) {
//                minBlock = middle;
//            } else {
//                maxBlock = middle;
//            }
//        }
//        return maxBlock;
//    }
//
//    private static boolean isUpdated(int blockHeight) throws SQLException {
//        List<BtcTransaction> txnList = queryTransaction.getTransactionsInBlock(blockHeight);
//        if (txnList != null && !txnList.isEmpty()) {
//            return txnList.stream().noneMatch((t) -> (t.getTransactionId() == 0));
//        }
//        throw new IllegalStateException("No transactions found in DB for block " + blockHeight);
//    }
    @SuppressWarnings("CallToPrintStackTrace")
    private static void updateDb(int nThreads, int firstBlock, int numBlocks, int pLastTransactionId) throws SQLException {
        log.info("updateDb(): nThreads={}, firstBlock={}, numBlocks={}, pLastTransactionId={}", nThreads, firstBlock, numBlocks, pLastTransactionId);
        int lastTransactionId = 0;
        if (firstBlock > 1) {
            for (BtcTransaction t : queryTransaction.getTransactionsInBlock(firstBlock - 1)) {
                if (t.getTransactionId() > lastTransactionId) {
                    lastTransactionId = t.getTransactionId();
                }
            }
        }
        log.info("lastTransactionId={}", lastTransactionId);
        ParallelGetBlock parallelGetBlock = new ParallelGetBlock(nThreads, firstBlock, numBlocks);
        try (DbUpdateTransaction addTxn = new DbUpdateTransaction(conn)) {
            for (int i = 0; i < numBlocks; i++) {
                long s = System.currentTimeMillis();
                int blockHeight = firstBlock + i;
                Block block = parallelGetBlock.getBlock(blockHeight);
                try {
                    lastTransactionId = updateBlock(block, lastTransactionId, addTxn, pLastTransactionId);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                log.info(i + ". Block height: " + blockHeight
                        + ",\t numTxns=" + block.tx().size()
                        + ",\t Queue.size=" + parallelGetBlock.getQueueSize()
                        + ".\t  Runtime: " + (System.currentTimeMillis() - s) + " ms.");
            }
        }
    }

    private static int updateBlock(Block block, int lastTransactionId, DbUpdateTransaction addTxn, int pLastTransactionId) throws SQLException {
        for (String txid : block.tx()) {
            lastTransactionId++;
            if (lastTransactionId > pLastTransactionId) {
//                addTxn.updateId(Utils.fixDupeTxid(txid, block.height()), lastTransactionId, block.height());
            }
        }
        return lastTransactionId;
    }
}
