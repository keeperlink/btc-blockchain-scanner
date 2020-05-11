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
import com.sliva.btc.scanner.db.DbCachedAddress;
import com.sliva.btc.scanner.db.DbQueryBlock;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryOutput;
import com.sliva.btc.scanner.db.DbQueryTransaction;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.db.model.TxInput;
import com.sliva.btc.scanner.db.model.TxOutput;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class TestRawBlock {

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        log.debug("START");
        DBConnectionSupplier conn = new DBConnectionSupplier("btc");
        DbQueryBlock queryBlock = new DbQueryBlock(conn);
        DbQueryTransaction queryTransaction = new DbQueryTransaction(conn);
        DbQueryInput queryInput = new DbQueryInput(conn);
        DbQueryOutput queryOutput = new DbQueryOutput(conn);
        DbCachedAddress queryAddress = new DbCachedAddress(conn);

        NetworkParameters np = new MainNetParams();
        List<File> blockChainFiles = new ArrayList<>();
        blockChainFiles.add(new File("C:\\CryptoData\\Bitcoin\\blocks\\blk00000.dat"));
        BlockFileLoader bfl = new BlockFileLoader(np, blockChainFiles);

        Context context = new Context(np);
        log.trace("context:{}", context);
        String skipToBlock = "00000000000561f7d0e08fbcd1d8cab38477da5a78af9a1e0f3a376e061ca947";
        boolean searching = true;
        for (Block block : bfl) {
            log.info("Block.hash: " + block.getHashAsString());
            if (searching && !block.getHashAsString().equals(skipToBlock)) {
                continue;
            }
            searching = false;
            if ("00000000000561f7d0e08fbcd1d8cab38477da5a78af9a1e0f3a376e061ca947".equals(block.getHashAsString())) {
                continue;
            }
            BtcBlock btcBlock = queryBlock.findBlockByHash(block.getHashAsString()).orElseThrow(() -> new IllegalStateException("Block not found in DB: " + block.getHashAsString()));
            log.info("Block.height: " + btcBlock.getHeight());
            List<BtcTransaction> listTxn = queryTransaction.getTransactionsInBlock(btcBlock.getHeight());
            List<Transaction> transactions = block.getTransactions();
            if (transactions != null) {
                log.info("getTransactions: " + transactions.size());
                for (Transaction t : transactions) {
                    log.info("Tx.hash: " + t.getHashAsString());
                    BtcTransaction btcTransaction = findTx(listTxn, t.getHashAsString());
                    if (btcTransaction == null) {
                        throw new IllegalStateException("Transaction not found in DB: " + t.getHashAsString());
                    }
                    log.info("Tx.inputs: " + t.getInputs().size());
                    List<TxInput> txInputs = queryInput.getInputs(btcTransaction.getTransactionId());
                    for (TransactionInput ti : t.getInputs()) {
//                    log.info("\tIn.isCoinBase: " + ti.isCoinBase());
                        if (!ti.isCoinBase()) {
                            log.info("\tIn.Outpoint: " + ti.getOutpoint().getHash().toString() + ":" + ti.getOutpoint().getIndex());
                            log.info("\tIn.Value: " + ti.getValue());
                            BtcTransaction inTxn = queryTransaction.findTransaction(ti.getOutpoint().getHash().toString())
                                    .orElseThrow(() -> new IllegalStateException("Transaction not found in DB: " + ti.getOutpoint().getHash().toString()));
                            TxInput txInput = findInput(txInputs, inTxn.getTransactionId(), (int) ti.getOutpoint().getIndex());
                            if (txInput == null) {
                                throw new IllegalStateException("Transaction INPUT not found in DB: " + ti.getOutpoint().getHash().toString() + ":" + ti.getOutpoint().getIndex());
                            }
                        }
                    }
                    if (!txInputs.isEmpty()) {
                        throw new IllegalStateException("More inputs in DB: " + txInputs);
                    }
                    log.info("Tx.outputs: " + t.getOutputs().size());
                    List<TxOutput> txOutputs = queryOutput.getOutputs(btcTransaction.getTransactionId());
                    t.getOutputs().stream().forEach((to) -> {
                        log.info("\tOut.amount: " + to.getValue().longValue());
                        try {
                            String addrStr = to.getScriptPubKey().getToAddress(np, true).toString();
                            log.info("\tOut.addr: " + addrStr);
                            TxOutput txOutput = findOutput(txOutputs, to.getIndex());
                            if (txOutput == null) {
                                throw new IllegalStateException("Transaction OUTPUT not found in DB: " + t.getHashAsString() + ":" + to.getIndex());
                            }
                            if (txOutput.getAmount() != to.getValue().longValue()) {
                                throw new IllegalStateException("Value doesn't match with DB: " + to.getValue().longValue() + " <> " + txOutput.getAmount());
                            }
                            BtcAddress btcAddress = queryAddress.getAddress(txOutput.getAddressId(), true).orElseThrow(() -> new IllegalStateException("Address not found in DB: " + txOutput.getAddressId()));
                            if (!btcAddress.getAddress().equals(addrStr)) { //TODO wrong comare
                                throw new IllegalStateException("Address doesn't match with DB: " + addrStr + " <> " + btcAddress.getAddress());
                            }
                        } catch (Exception e) {
                            if (to.getValue().longValue() == 0) {
                                log.info("\tOut.addr: 000000000000000000000000000");
                            } else {
                                e.printStackTrace();
                            }
                        }
                    });
                    if (!txOutputs.isEmpty()) {
//                        for (Iterator<TxOutput> i = txOutputs.iterator(); i.hasNext();) {
//                            TxOutput o = i.next();
//                            BtcAddress btcAddress = queryAddress.getAddress(o.getAddressId(), true);
//                        if (btcAddress != null && btcAddress.getAddress().startsWith("z+")) {
//                            i.remove();
//                        }
//                        }
                        if (!txOutputs.isEmpty()) {
                            throw new IllegalStateException("More outputs in DB: " + txOutputs);
                        }
                    }
                }
            }
            if (!listTxn.isEmpty()) {
                throw new IllegalStateException("More transactions in DB: " + listTxn);
            }
        }
    }

    private static BtcTransaction findTx(List<BtcTransaction> list, String hash) {
        for (BtcTransaction t : list) {
            if (t.getTxid().equalsIgnoreCase(hash)) {
                list.remove(t);
                return t;
            }
        }
        return null;
    }

    private static TxInput findInput(List<TxInput> list, int transactionId, int pos) {
        for (TxInput t : list) {
            if (t.getInTransactionId() == transactionId && t.getInPos() == pos) {
                list.remove(t);
                return t;
            }
        }
        return null;
    }

    private static TxOutput findOutput(List<TxOutput> list, int pos) {
        for (TxOutput t : list) {
            if (t.getPos() == pos) {
                list.remove(t);
                return t;
            }
        }
        return null;
    }
}
