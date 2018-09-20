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

import com.sliva.btc.scanner.rpc.RpcClient;
import com.sliva.btc.scanner.db.DBConnection;
import com.sliva.btc.scanner.db.DbUpdateInput;
import com.sliva.btc.scanner.db.DbCachedOutput;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryInput.TxInputOutput;
import com.sliva.btc.scanner.db.DbQueryOutput;
import com.sliva.btc.scanner.db.DbQueryOutput.TxOutputInput;
import com.sliva.btc.scanner.db.DbQueryTransaction;
import com.sliva.btc.scanner.db.DbUpdateOutput;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.db.model.OutputStatus;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import static org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5;
import org.apache.commons.io.FileUtils;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.In;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.Out;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class FixAddresses {
//
//    private static BtcClient client;
//    private static final DBConnection conn = new DBConnection("btc2");
//
//    /**
//     * @param args the command line arguments
//     * @throws java.lang.Exception
//     */
//    public static void main(String[] args) throws Exception {
//        DbUpdateAddress.MAX_BATCH_SIZE = 1;
//        DbUpdateOutput.MAX_INSERT_QUEUE_LENGTH = 1;
//        DbUpdateInput.MAX_INSERT_QUEUE_LENGTH = 1;
//        client = new BtcClient();
//        BitcoindRpcClient.BlockChainInfo bci = client.getClient().getBlockChainInfo();
//        log.info("BlockChainInfo: " + bci);
//        DbQueryTransaction queryTransaction = new DbQueryTransaction(conn);
//        DbQueryInput queryInput = new DbQueryInput(conn);
//        DbQueryOutput queryOutput = new DbQueryOutput(conn);
//        try (DbUpdateInput updateInput = new DbUpdateInput(conn);
//                DbCachedOutput cachedOutput = new DbCachedOutput(conn);
//                DbCachedAddress cachedAddress = new DbCachedAddress(conn)) {
//            int nLoop = 0;
//            for (Range r : getRanges(loadNumbersFromFile(new File("/tmp/missing-output-addr.txt")), 0)) {
//                log.info("Range: " + r);
//                for (BtcTransaction t : queryTransaction.getTxnsRangle(r.getMin(), r.getMax())) {
//                    if (++nLoop % 1000 == 0) {
//                        log.info("LOOP: " + nLoop);
//                    }
//                    RawTransaction rt = client.getRawTransaction(Utils.unfixDupeTxid(t.getTxid()));
//                    boolean isCoinBaseTxn = rt.vIn() == null || rt.vIn().isEmpty() || rt.vIn().get(0).txid() == null;
//                    int nInputs = isCoinBaseTxn ? 0 : rt.vIn().size();
//                    int nOutputs = rt.vOut().size();
//                    if (t.getNInputs() != nInputs || t.getNOutputs() != nOutputs) {
//                        log.info("tx inputs/outputs numbers do not match: expected: inputs=" + nInputs + ", outputs=" + nOutputs + ". In DB: " + t);
//                    }
////                    updateTxn.updateInOut(t.getTransactionId(), nInputs, );
//                    if (!isCoinBaseTxn) {
//                        List<TxInputOutputAddr> existingDbRecords = queryInput.getInputsOutputAddr(t.getTransactionId());
//                        int n = 0;
//                        for (In in : rt.vIn()) {
//                            final int N = n++;
//                            TxInputOutputAddr rec = existingDbRecords == null ? null : existingDbRecords.stream().filter((a) -> a.getInput().getPos() == N).findAny().orElse(null);
//                            if (rec != null) {
//                                //log.info("rec=" + rec);
//                                if (rec.getAddress() == null) {
//                                    log.info("INPUT: Missing address record: " + rec);
//                                    if (rec.getOutput() != null) {
//                                        try {
//                                            BtcTransaction corrOutTx = queryTransaction.findTransaction(rec.getOutput().getTransactionId());
//                                            log.info("INPUT: corrOutTx=" + corrOutTx);
//                                            RawTransaction rtCorr = client.getRawTransaction(Utils.unfixDupeTxid(corrOutTx.getTxid()));
//                                            String address;
//                                            if (rtCorr.vOut().get(rec.getOutput().getPos()).scriptPubKey().addresses() == null) {
//                                                address = "z+" + new DigestUtils(MD5).digestAsHex(rtCorr.vOut().get(rec.getOutput().getPos()).scriptPubKey().asm());
//                                            } else {
//                                                address = rtCorr.vOut().get(rec.getOutput().getPos()).scriptPubKey().addresses().get(0);
//                                            }
//                                            log.info("INPUT: extracted by rpc address: " + address);
//                                            int dbadr = cachedAddress.getOrAdd(address, true);
//                                            log.info("INPUT: extracted by rpc address: dbadr=" + dbadr);
//                                            if (dbadr != 0) {
//                                                if (dbadr != rec.getInput().getAddressId()) {
//                                                    log.info("INPUT: Updating input address: " + rec.getInput().getAddressId() + " ==> " + dbadr);
//                                                    updateInput.updateAddress(rec.getInput().getTransactionId(), rec.getInput().getPos(), dbadr);
//                                                }
//                                                if (dbadr != rec.getOutput().getAddressId()) {
//                                                    log.info("INPUT: Updating output address: " + rec.getOutput().getAddressId() + " ==> " + dbadr);
//                                                    cachedOutput.updateAddress(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), dbadr);
//                                                }
//                                            }
//                                        } catch (Exception e) {
//                                            e.printStackTrace();
//                                        }
//                                    }
//                                }
//                                if (rec.getOutput() == null) {
//                                    log.info("INPUT: Missing correcponding output record: " + rec);
//                                } else {
//                                    if (rec.getInput().getAmount() != rec.getOutput().getAmount()) {
//                                        log.info("INPUT: In/Out amount mismatch: In:" + rec.getInput().getAmount() + " <> Out:" + rec.getOutput().getAmount() + ".  Rec: " + rec);
//                                    }
//                                    if (rec.getInput().getAddressId() != rec.getOutput().getAddressId()) {
//                                        log.info("INPUT: In/Out Address mismatch: " + rec);
//                                        try {
//                                            BtcTransaction corrOutTx = queryTransaction.findTransaction(rec.getOutput().getTransactionId());
//                                            RawTransaction rtCorr = client.getRawTransaction(Utils.unfixDupeTxid(corrOutTx.getTxid()));
//                                            String address = rtCorr.vOut().get(rec.getOutput().getPos()).scriptPubKey().addresses().get(0);
//                                            log.info("INPUT: extracted by rpc address: " + address);
//                                            int dbadr = cachedAddress.getOrAdd(address, true);
//                                            log.info("INPUT: extracted by rpc address: dbadr=" + dbadr);
//                                            if (dbadr != 0) {
//                                                if (dbadr != rec.getInput().getAddressId()) {
//                                                    log.info("INPUT: Updating input address: " + rec.getInput().getAddressId() + " ==> " + dbadr);
//                                                    updateInput.updateAddress(rec.getInput().getTransactionId(), rec.getInput().getPos(), dbadr);
//                                                }
//                                                if (dbadr != rec.getOutput().getAddressId()) {
//                                                    log.info("INPUT: Updating output address: " + rec.getOutput().getAddressId() + " ==> " + dbadr);
//                                                    cachedOutput.updateAddress(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), dbadr);
//                                                }
//                                            }
//                                        } catch (Exception e) {
//                                            e.printStackTrace();
//                                        }
//                                    }
//                                    if (rec.getOutput().getStatus() != OutputStatus.SPENT) {
////                                        log.info("Correcponding Output is not marked as spent: " + rec);
////                                        cachedOutput.updateSpent(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), Boolean.TRUE);
//                                    }
//                                }
//                            } else {
//                                log.info("INPUT: DB missing input: " + in.txid() + ":" + N);
//                                int inTransactionId = queryTransaction.findTransactionId(in.txid());
//                                if (inTransactionId == 0) {
//                                    throw new IllegalStateException("Cannot find transactionID for txid " + in.txid());
//                                }
//                                TxOutput txOutput = cachedOutput.getOutput(inTransactionId, in.vout());
//                                if (txOutput == null) {
//                                    throw new IllegalStateException("Cannot find output: " + inTransactionId + ":" + in.vout());
//                                }
////                                addInput.add(TxInput.builder()
////                                        .transactionId(t.getTransactionId())
////                                        .pos(n++)
////                                        .inTransactionId(inTransactionId)
////                                        .inPos(in.vout())
////                                        .addressId(txOutput.getAddressId())
////                                        .amount(txOutput.getAmount())
////                                        .build());
////                                cachedOutput.updateSpent(inTransactionId, in.vout(), Boolean.TRUE);
//                            }
//                        }
//                    }
//                    List<TxOutputInputAddr> existingDbRecords = queryOutput.getOutputsInputAddr(t.getTransactionId());
//                    for (Out out : rt.vOut()) {
//                        TxOutputInputAddr rec = existingDbRecords == null ? null : existingDbRecords.stream().filter((a) -> a.getOutput().getPos() == out.n()).findAny().orElse(null);
//                        String address;
//                        if (out.scriptPubKey().addresses() != null && !out.scriptPubKey().addresses().isEmpty()) {
//                            address = out.scriptPubKey().addresses().get(0);
//                        } else {
//                            address = "z+" + new DigestUtils(MD5).digestAsHex(out.scriptPubKey().asm());
//                        }
//                        if (rec != null) {
//                            if (rec.getAddress() == null) {
//                                if (rec.getOutput().getAddressId() != 0) {
//                                    log.info("OUTPUT: Missing address record: address=" + address + ".  Rec: " + rec);
//                                    BtcAddress dbadr = cachedAddress.getAddress(address, true);
//                                    if (dbadr == null) {
//                                        cachedAddress.add(BtcAddress.builder()
//                                                .addressId(rec.getOutput().getAddressId())
//                                                .address(address)
//                                                .build(), true);
//                                    } else if (dbadr.getAddressId() != rec.getOutput().getAddressId()) {
//                                        log.info("OUTPUT: Found address " + address + " in DB with different ID: " + dbadr);
//                                        cachedOutput.updateAddress(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), dbadr.getAddressId());
//                                        if (rec.getInput() != null) {
//                                            updateInput.updateAddress(rec.getInput().getTransactionId(), rec.getInput().getPos(), dbadr.getAddressId());
//                                        }
//                                    }
//                                } else {
//                                    log.info("OUTPUT: extracted by rpc address: " + address);
//                                    int dbadr = cachedAddress.getOrAdd(address, true);
//                                    log.info("OUTPUT: extracted by rpc address: dbadr=" + dbadr);
//                                    if (dbadr != 0) {
//                                        if (dbadr != rec.getInput().getAddressId()) {
//                                            log.info("OUTPUT: Updating input address: " + rec.getInput().getAddressId() + " ==> " + dbadr);
//                                            updateInput.updateAddress(rec.getInput().getTransactionId(), rec.getInput().getPos(), dbadr);
//                                        }
//                                        if (dbadr != rec.getOutput().getAddressId()) {
//                                            log.info("OUTPUT: Updating output address: " + rec.getOutput().getAddressId() + " ==> " + dbadr);
//                                            cachedOutput.updateAddress(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), dbadr);
//                                        }
//                                    }
//                                }
//                            } else if (!address.equals(rec.getAddress().getAddress())) {
//                                log.info("OUTPUT: Address is different: DB:" + rec.getAddress().getAddress() + ", rpc:" + address + ".  Rec: " + rec);
//                                int dbadr = cachedAddress.getOrAdd(address, true);
//                                log.info("OUTPUT: dbadr=" + dbadr);
//                                if (dbadr != 0) {
//                                    if (dbadr != rec.getInput().getAddressId()) {
//                                        log.info("OUTPUT: Updating input address: " + rec.getInput().getAddressId() + " ==> " + dbadr);
//                                        updateInput.updateAddress(rec.getInput().getTransactionId(), rec.getInput().getPos(), dbadr);
//                                    }
//                                    if (dbadr != rec.getOutput().getAddressId()) {
//                                        log.info("UOUTPUT: pdating output address: " + rec.getOutput().getAddressId() + " ==> " + dbadr);
//                                        cachedOutput.updateAddress(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), dbadr);
//                                    }
//                                }
//                            }
//                            if (rec.getInput().getAmount() != rec.getOutput().getAmount()) {
//                                log.info("OUTPUT: In/Out amount mismatch: " + rec);
//                            }
//                            if (rec.getInput().getAddressId() != rec.getOutput().getAddressId()) {
//                                log.info("OUTPUT: In/Out Address mismatch: " + rec);
//                                int dbadr = cachedAddress.getOrAdd(address, true);
//                                log.info("OUTPUT: dbadr=" + dbadr);
//                                if (dbadr != 0) {
//                                    if (dbadr != rec.getInput().getAddressId()) {
//                                        log.info("OUTPUT: Updating input address: " + rec.getInput().getAddressId() + " ==> " + dbadr);
//                                        updateInput.updateAddress(rec.getInput().getTransactionId(), rec.getInput().getPos(), dbadr);
//                                    }
//                                    if (dbadr != rec.getOutput().getAddressId()) {
//                                        log.info("OUTPUT: Updating output address: " + rec.getOutput().getAddressId() + " ==> " + dbadr);
//                                        cachedOutput.updateAddress(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), dbadr);
//                                    }
//                                }
//                            }
//                            if (rec.getOutput().getStatus() != OutputStatus.SPENT) {
////                                    log.info("Output is not marked as spent: " + rec);
////                                    cachedOutput.updateSpent(rec.getOutput().getTransactionId(), rec.getOutput().getPos(), Boolean.TRUE);
//                            }
//                        } else {
//                            log.info("OUTPUT: DB missing output: " + rt.txId() + ":" + out.n());
////                            int addressId = cachedAddress.getOrAdd(address);
////                            cachedOutput.add(TxOutput.builder()
////                                    .transactionId(t.getTransactionId())
////                                    .pos(out.n())
////                                    .addressId(addressId)
////                                    .amount(Math.round(out.value()*1e8))
////                                    .spent(Boolean.FALSE)
////                                    .build());
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    private static List<Range> getRanges(List<Integer> numbersList, int rangeExtend) {
//        List<Range> result = new ArrayList<>();
//        if (numbersList.isEmpty()) {
//            return result;
//        }
//        Collections.sort(numbersList);
//        int beginRange = numbersList.get(0);
//        for (int i = 1; i < numbersList.size(); i++) {
//            int curNum = numbersList.get(i);
//            int prevNum = numbersList.get(i - 1);
//            if (curNum - prevNum > rangeExtend * 2) {
//                result.add(new Range(Math.max(0, beginRange - rangeExtend), prevNum + rangeExtend));
//                beginRange = curNum;
//            }
//        }
//        result.add(new Range(Math.max(0, beginRange - rangeExtend), numbersList.get(numbersList.size() - 1) + rangeExtend));
//        return result;
//    }
//
//    private static List<Integer> loadNumbersFromFile(File f) throws IOException {
//        List<Integer> result = new ArrayList<>();
//        for (String s : FileUtils.readLines(f, StandardCharsets.UTF_8)) {
//            s = s.trim();
//            if (!s.isEmpty()) {
//                result.add(Integer.valueOf(s));
//            }
//        }
//        return result;
//
//    }
//
//    @Getter
//    @AllArgsConstructor
//    @ToString
//    private static class Range {
//
//        private final int min;
//        private final int max;
//    }
}
