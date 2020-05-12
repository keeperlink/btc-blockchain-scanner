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
import com.sliva.btc.scanner.db.DbUpdateBlock;
import com.sliva.btc.scanner.db.DbQueryBlock;
import com.sliva.btc.scanner.db.DbQueryTransaction;
import com.sliva.btc.scanner.db.DbUpdateTransaction;
import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.rpc.ParallelGetBlock;
import com.sliva.btc.scanner.rpc.RpcClient;
import com.sliva.btc.scanner.util.Utils;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Block;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunScan {

    private static final int RPC_THREADS = 70;
    private static final int FIRST_BLOCK = 540477;
    private static final int NUM_BLOCKS = -1000;
    private static final boolean RUN_SAFE_MODE = true;
    private static final boolean UPDATE_LATEST = true;
    private static BitcoinJSONRPCClient client;
    private static final DBConnectionSupplier conn = new DBConnectionSupplier();
    private static DbQueryBlock queryBlock;
    private static DbQueryTransaction queryTransaction;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        log.debug("START");
        client = new RpcClient().getClient();
        queryBlock = new DbQueryBlock(conn);
        queryTransaction = new DbQueryTransaction(conn);
        BitcoindRpcClient.BlockChainInfo bci = client.getBlockChainInfo();
        log.info("BlockChainInfo: {}", bci);
        if (UPDATE_LATEST) {
            int firstBlock = queryBlock.findLastHeight().orElse(-1) + 1;
            int numBlocks = bci.blocks() - firstBlock + 1;
            log.info("firstBlock={}, numBlocks={}", firstBlock, numBlocks);
            if (numBlocks > 0) {
                updateDb(1, firstBlock, numBlocks);
            }
        } else {
            updateDb(RPC_THREADS, FIRST_BLOCK, NUM_BLOCKS);
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    private static void updateDb(int nThreads, int firstBlock, int numBlocks) throws SQLException {
        ParallelGetBlock parallelGetBlock = new ParallelGetBlock(nThreads, firstBlock, numBlocks);
        try (DbUpdateBlock addBlock = new DbUpdateBlock(conn);
                DbUpdateTransaction addTxn = new DbUpdateTransaction(conn)) {
            for (int i = 0; i < Math.abs(numBlocks); i++) {
                long s = System.currentTimeMillis();
                int blockHeight = numBlocks < 0 ? firstBlock - i : firstBlock + i;
//                log.info(i + ". Block height: " + blockHeight);
                Block block = parallelGetBlock.getBlock(blockHeight);
                try {
                    saveBlock(block, addBlock, addTxn);
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

    private static void saveBlock(Block block, DbUpdateBlock addBlock, DbUpdateTransaction addTxn) throws SQLException {
//        log.info("block.hash=" + block.hash());
//        log.info("block.nTXNs=" + block.tx().size());
        List<String> txnsInDb = null;
        if (!RUN_SAFE_MODE || queryBlock.getBlockHash(block.height()) == null) {
            try {
                addBlock.add(BtcBlock.builder().height(block.height()).hash(block.hash()).txnCount(block.tx().size()).build());
            } catch (Exception e) {
                log.info("saveBlock(): Err: " + e.getClass() + ": " + e.getMessage());
                queryBlock.getBlock(block.height()).orElseThrow(() -> new SQLException(e));
                txnsInDb = queryTransaction.getTxnsInBlock(block.height());
                if (txnsInDb != null && txnsInDb.size() == block.tx().size()) {
                    return;
                }
            }
        }
        if (RUN_SAFE_MODE && txnsInDb == null) {
            txnsInDb = queryTransaction.getTxnsInBlock(block.height());
        }
        for (String txid : block.tx()) {
            txid = Utils.fixDupeTxid(txid, block.height());
            if (txnsInDb == null || !txnsInDb.contains(txid)) {
//                log.info("addTxn: " + txid + ", block: " + block.height());
                addTxn.add(BtcTransaction.builder()
                        .txid(txid)
                        .blockHeight(block.height())
                        .build());
            }
        }
    }
}
