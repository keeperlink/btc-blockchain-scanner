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

import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class FillInputsOutputs {
//
//    private static final int NUM_RPC_THREADS = 100;
//    private static final int NUM_TRANS = 1800000;
//    private static final boolean RUN_SAFE = false;
//    private static BtcClient client;
//    private static final DBConnection conn = new DBConnection("btc");
//
//    /**
//     * @param args the command line arguments
//     * @throws java.lang.Exception
//     */
//    public static void main(String[] args) throws Exception {
//
//        client = new BtcClient();
//        BitcoindRpcClient.BlockChainInfo bci = client.getClient().getBlockChainInfo();
//        log.info("BlockChainInfo: " + bci);
//        DbQueryTransaction queryTransaction = new DbQueryTransaction(conn);
//        DbQueryInput queryInput = new DbQueryInput(conn);
//        DbQueryOutput queryOutput = new DbQueryOutput(conn);
//        List<BtcTransaction> transactions = queryTransaction.getTxnsNoOutputs(NUM_TRANS);
//
////        List<BtcTransaction> transactions = queryTransaction.getTxnsRangle(141000, 142500);
////        transactions.clear();
////        transactions.add(queryTransaction.findTransaction(97258));
//        log.info("Transactions loaded: " + (transactions == null ? null : transactions.size()));
//        if (transactions == null || transactions.isEmpty()) {
//            return;
//        }
//        ParallelGetRawTransaction parallelGetRawTransaction = new ParallelGetRawTransaction(NUM_RPC_THREADS, transactions);
//        try (DbUpdateInput addInput = new DbUpdateInput(conn);
//                DbCachedOutput cachedOutput = new DbCachedOutput(conn);
//                DbCachedAddress cachedAddress = new DbCachedAddress(conn);
//                DbUpdateTransaction updateTxn = new DbUpdateTransaction(conn)) {
//            int nLoop = 0;
//            for (BtcTransaction t : transactions) {
//                if (++nLoop % 200 == 0) {
//                    log.info("LOOP: " + nLoop + " / " + transactions.size() + ", queue.size=" + parallelGetRawTransaction.getQueueSize());
//                }
////                log.info("getTransaction: " + Utils.unfixDupeTxid(t.getTxid()));
//                RawTransaction rt = parallelGetRawTransaction.getRawTransaction(Utils.unfixDupeTxid(t.getTxid()));
//                boolean isCoinBaseTxn = rt.vIn() == null || rt.vIn().isEmpty() || rt.vIn().get(0).txid() == null;
//                updateTxn.updateInOut(t.getTransactionId(), isCoinBaseTxn ? 0 : rt.vIn().size(), rt.vOut().size());
//                if (!isCoinBaseTxn) {
//                    List<TxInput> existingDbRecords = null;
//                    if (RUN_SAFE) {
//                        existingDbRecords = queryInput.getInputs(t.getTransactionId());
//                    }
//                    int n = 0;
//                    for (In in : rt.vIn()) {
//                        final int N = n;
//                        if (existingDbRecords != null && existingDbRecords.stream().anyMatch((a) -> a.getPos() == N)) {
//                            continue;
//                        }
//                        int inTransactionId = queryTransaction.findTransactionId(in.txid());
//                        if (inTransactionId == 0) {
//                            throw new IllegalStateException("Cannot find transactionID for txid " + in.txid());
//                        }
//                        TxOutput txOutput = cachedOutput.getOutput(inTransactionId, in.vout());
//                        if (txOutput == null) {
//                            throw new IllegalStateException("Cannot find output: " + inTransactionId + ":" + in.vout());
//                        }
//                        addInput.add(TxInput.builder()
//                                .transactionId(t.getTransactionId())
//                                .pos(n++)
//                                .inTransactionId(inTransactionId)
//                                .inPos(in.vout())
//                                .addressId(txOutput.getAddressId())
//                                .amount(txOutput.getAmount())
//                                .build());
//                        cachedOutput.updateStatus(inTransactionId, in.vout(), OutputStatus.SPENT);
//                    }
//                }
//                List<TxOutput> existingDbRecords = null;
//                if (RUN_SAFE) {
//                    existingDbRecords = queryOutput.getOutputs(t.getTransactionId());
//                }
//                for (Out out : rt.vOut()) {
//                    if (existingDbRecords != null && existingDbRecords.stream().anyMatch((a) -> a.getPos() == out.n())) {
//                        continue;
//                    }
//                    String address;
//                    if (out.scriptPubKey().addresses() != null && !out.scriptPubKey().addresses().isEmpty()) {
//                        address = out.scriptPubKey().addresses().get(0);
//                    } else {
//                        address = "z+" + new DigestUtils(MD5).digestAsHex(out.scriptPubKey().asm());
//                    }
//                    int addressId = cachedAddress.getOrAdd(address, true);
//                    cachedOutput.add(TxOutput.builder()
//                            .transactionId(t.getTransactionId())
//                            .pos(out.n())
//                            .addressId(addressId)
//                            .amount(Math.round(out.value() * 1e8))
//                            .status(OutputStatus.UNDEFINED)
//                            .build());
//                }
//            }
//        }
//    }
}
