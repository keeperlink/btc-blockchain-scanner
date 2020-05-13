/*
 * Copyright 2020 Sliva Co.
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
package com.sliva.btc.scanner.db;

import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.util.ThreadFactoryWithDBConnection;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public final class DbValidationUtils {

    /**
     * Check last few blocks, transactions, inputs and outputs are complete
     *
     * @param dbCon
     */
    @SneakyThrows({InterruptedException.class, ExecutionException.class})
    public static void checkAndFixDataTails(DBConnectionSupplier dbCon) {
        int VALIDATE_LAST_TRANSACTIONS_NUMBER = 60000;
        StopWatch start = StopWatch.createStarted();
        ForkJoinPool execBlocks = new ForkJoinPool(4, new ThreadFactoryWithDBConnection(dbCon, "execBlocks", false), null, false);
        ForkJoinPool execTrans = new ForkJoinPool(6, new ThreadFactoryWithDBConnection(dbCon, "execTrans", false), null, false);
        DbQueryBlock queryBlock = new DbQueryBlock(dbCon);
        DbQueryTransaction queryTransaction = new DbQueryTransaction(dbCon);
        DbQueryInput queryInput = new DbQueryInput(dbCon);
        DbQueryOutput queryOutput = new DbQueryOutput(dbCon);
        DbQueryAddressCombo queryAddress = new DbQueryAddressCombo(dbCon);
        try (DbUpdateBlock updateBlock = new DbUpdateBlock(dbCon);
                DbUpdateInput updateInput = new DbUpdateInput(dbCon);
                DbUpdateInputSpecial updateInputSpecial = new DbUpdateInputSpecial(dbCon);
                DbUpdateTransaction updateTxn = new DbUpdateTransaction(dbCon);
                DbUpdateOutput updateOutput = new DbUpdateOutput(dbCon)) {
            Optional<Integer> olastBlockInTable = queryBlock.findLastHeight();
            Optional<BtcTransaction> olastTxInTable = queryTransaction.getLastTransaction();
            if (!olastBlockInTable.isPresent() || !olastTxInTable.isPresent()) {
                DBUtils.truncateTables(dbCon, updateBlock, updateTxn, updateInput, updateInputSpecial, updateOutput);
            } else {
                int lastTransactionId = olastTxInTable.get().getTransactionId();
                int firstTransactionId = Math.max(0, lastTransactionId - VALIDATE_LAST_TRANSACTIONS_NUMBER);
                List<BtcTransaction> latestTransactions = queryTransaction.getTxnsRangle(firstTransactionId, lastTransactionId);
                int firstBlockHeight = latestTransactions.stream().mapToInt(BtcTransaction::getBlockHeight).min().getAsInt() + 1;
                int lastBlockHeight = olastBlockInTable.get();
                log.debug("Validating {} blocks: [{}..{}]", lastBlockHeight - firstBlockHeight + 1, firstBlockHeight, lastBlockHeight);
                AtomicInteger earliestBadBlockHeight = new AtomicInteger(Integer.MAX_VALUE);
                execBlocks.submit(() -> IntStream.rangeClosed(firstBlockHeight, lastBlockHeight).parallel().forEach(height -> {
                    Optional<BtcBlock> oBlock = queryBlock.getBlock(height);
                    if (oBlock.isPresent()) {
                        Integer aheight = height;
                        long txRecordsInTable = latestTransactions.stream().mapToInt(BtcTransaction::getBlockHeight).filter(aheight::equals).count();
                        if (oBlock.get().getTxnCount() != txRecordsInTable) {
                            earliestBadBlockHeight.accumulateAndGet(height, (n1, n2) -> Math.min(n1, n2));
                        } else {
                            try {
                                execTrans.submit(() -> latestTransactions.stream().parallel().filter(tx -> tx.getBlockHeight() == height).forEach(tx -> {
                                    if (tx.getBlockHeight() < earliestBadBlockHeight.get()) {
                                        int inputsInTable = queryInput.countInputsInTransaction(tx.getTransactionId());
                                        int outputsInTable = queryOutput.countOutputsInTransaction(tx.getTransactionId());
                                        if (tx.getNInputs() != inputsInTable || tx.getNOutputs() != outputsInTable) {
                                            earliestBadBlockHeight.accumulateAndGet(tx.getBlockHeight(), (n1, n2) -> Math.min(n1, n2));
                                        } else {
                                            //check outputs for missing address records
                                            queryOutput.getOutputs(tx.getTransactionId()).stream()
                                                    .filter(to -> to.getAddressId() != 0)
                                                    .filter(to -> !queryAddress.findByAddressId(to.getAddressId()).isPresent())
                                                    .findAny().ifPresent(
                                                            to -> earliestBadBlockHeight.accumulateAndGet(to.getTransactionId(), (n1, n2) -> Math.min(n1, n2)));
                                        }
                                    }
                                })).get();
                            } catch (ExecutionException | InterruptedException ex) {
                                log.error(null, ex);
                            }
                        }
                    } else {
                        earliestBadBlockHeight.accumulateAndGet(height, (n1, n2) -> Math.min(n1, n2));
                    }
                })).get();
                //remove all starting from the bad block
                AtomicInteger deletedBlocks = new AtomicInteger();
                for (int height = earliestBadBlockHeight.get(); height <= lastBlockHeight; height++) {
                    if (updateBlock.delete(height)) {
                        deletedBlocks.incrementAndGet();
                    }
                }
                if (deletedBlocks.get() != 0) {
                    log.info("Deleted {} records from table {}", deletedBlocks.get(), updateBlock.getTableName());
                }
                olastBlockInTable = queryBlock.findLastHeight();
                if (!olastBlockInTable.isPresent()) {
                    //if blocks table is empty, then truncate all tables except addresses
                    DBUtils.truncateTables(dbCon, updateBlock, updateTxn, updateInput, updateInputSpecial, updateOutput);
                } else {
                    int lastBlockInBlocks = olastBlockInTable.get();
                    //delete transactions without block record
                    AtomicInteger deletedTx = new AtomicInteger();
                    latestTransactions.stream().filter(tx -> tx.getBlockHeight() > lastBlockInBlocks).forEach(tx -> {
                        if (updateTxn.delete(tx)) {
                            deletedTx.incrementAndGet();
                        }
                    });
                    if (deletedTx.get() != 0) {
                        log.info("Deleted {} records from table {}", deletedTx.get(), updateTxn.getTableName());
                    }

                    queryTransaction.getLastTransaction().ifPresent(tx -> {
                        int deleted = updateInput.deleteAllAboveTransactionId(tx.getTransactionId());
                        if (deleted != 0) {
                            log.info("Deleted {} records from table {}", deleted, updateInput.getTableName());
                        }
                        deleted = updateInputSpecial.deleteAllAboveTransactionId(tx.getTransactionId());
                        if (deleted != 0) {
                            log.info("Deleted {} records from table {}", deleted, updateInputSpecial.getTableName());
                        }
                        deleted = updateOutput.deleteAllAboveTransactionId(tx.getTransactionId());
                        if (deleted != 0) {
                            log.info("Deleted {} records from table {}", deleted, updateOutput.getTableName());
                        }
                    });
                }
            }
        } finally {
            execBlocks.shutdown();
            execTrans.shutdown();
            log.info("Pre-validation complete. Runtime: {}", Duration.ofNanos(start.getNanoTime()));
        }
    }

}
