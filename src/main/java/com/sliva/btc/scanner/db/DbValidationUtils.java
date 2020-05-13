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

import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.util.CommandLineUtils;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.ThreadFactoryWithDBConnection;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

/**
 * Perform latest records validation in tables "block", "transaction", "input",
 * "input_special" and "output" and remove any incomplete records.
 *
 * Data discrepancies might occur in case if last "update" process run did not
 * finish gracefully. The process designed to use schema with minimal set of
 * indexes to run "update" in fast (non-safe) mode.
 *
 * @author Sliva Co
 */
@Slf4j
public final class DbValidationUtils {

    private static final int DEFAULT_VALIDATE_LAST_TRANSACTIONS_NUMBER = 20000;
    public static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions();
    public static final CommandLineUtils.CmdOption dbVaidateTransactionsOpt = buildOption(CMD_OPTS, null, "db-validate-transactions", true, "Number of last transactions to validate for consistency. Default: " + DEFAULT_VALIDATE_LAST_TRANSACTIONS_NUMBER);
    private static int validateLastTransactionsNumber = DEFAULT_VALIDATE_LAST_TRANSACTIONS_NUMBER;

    public static void applyArguments(CommandLineUtils.CmdArguments cmdArguments) {
        validateLastTransactionsNumber = cmdArguments.getOption(dbVaidateTransactionsOpt).map(Integer::valueOf)
                .orElse(DEFAULT_VALIDATE_LAST_TRANSACTIONS_NUMBER);
        checkArgument(validateLastTransactionsNumber >= 0, "Value of parameter \"%s\" must be a non-negative number: %s", dbVaidateTransactionsOpt.getLongOpt(), validateLastTransactionsNumber);
    }

    /**
     * Check last few blocks, transactions, inputs and outputs are complete
     *
     * @param dbCon
     */
    @SneakyThrows({InterruptedException.class, ExecutionException.class})
    public static void checkAndFixDataTails(DBConnectionSupplier dbCon) {
        if (validateLastTransactionsNumber == 0) {
            return;
        }
        StopWatch start = StopWatch.createStarted();
        ForkJoinPool execBlocks = new ForkJoinPool(4, new ThreadFactoryWithDBConnection(dbCon, "execBlocks", false), null, false);
        ForkJoinPool execTrans = new ForkJoinPool(6, new ThreadFactoryWithDBConnection(dbCon, "execTrans", false), null, false);
        DbQueryBlock queryBlock = new DbQueryBlock(dbCon);
        DbQueryTransaction queryTransaction = new DbQueryTransaction(dbCon);
        DbQueryInput queryInput = new DbQueryInput(dbCon);
        DbQueryOutput queryOutput = new DbQueryOutput(dbCon);
        DbQueryAddressCombo queryAddress = new DbQueryAddressCombo(dbCon);
        try (DbUpdateBlock updateBlock = new DbUpdateBlock(dbCon);
                DbUpdateTransaction updateTxn = new DbUpdateTransaction(dbCon);
                DbUpdateInput updateInput = new DbUpdateInput(dbCon);
                DbUpdateInputSpecial updateInputSpecial = new DbUpdateInputSpecial(dbCon);
                DbUpdateOutput updateOutput = new DbUpdateOutput(dbCon)) {
            DbAccess db = new DbAccess(updateBlock, updateTxn, updateInput, updateInputSpecial, updateOutput);
            Optional<Integer> oLastBlockHeight = queryBlock.findLastHeight();
            Optional<BtcTransaction> olastTxInTable = queryTransaction.getLastTransaction();
            if (!oLastBlockHeight.isPresent() || !olastTxInTable.isPresent()) {
                DBUtils.truncateTables(dbCon, updateBlock, updateTxn, updateInput, updateInputSpecial, updateOutput);
            } else {
                int lastTransactionId = olastTxInTable.get().getTransactionId();
                int firstTransactionId = Math.max(0, lastTransactionId - validateLastTransactionsNumber - 1);
                List<BtcTransaction> latestTransactions = queryTransaction.getTxnsRangle(firstTransactionId, lastTransactionId);
                int firstBlockHeight = latestTransactions.stream().mapToInt(BtcTransaction::getBlockHeight).min().getAsInt() + 1;
                int lastBlockHeight = oLastBlockHeight.get();
                log.debug("Validating last {} blocks: [{}..{}]", lastBlockHeight - firstBlockHeight + 1, firstBlockHeight, lastBlockHeight);
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
                                execTrans.submit(() -> latestTransactions.stream().parallel()
                                        .filter(tx -> tx.getBlockHeight() == height)
                                        .filter(tx -> tx.getBlockHeight() < earliestBadBlockHeight.get())
                                        .forEach(tx -> {
                                            int inputsInTable = queryInput.countInputsByTransactionId(tx.getTransactionId());
                                            int outputsInTable = queryOutput.countOutputsByTransactionId(tx.getTransactionId());
                                            if (tx.getNInputs() != inputsInTable || tx.getNOutputs() != outputsInTable) {
                                                earliestBadBlockHeight.accumulateAndGet(tx.getBlockHeight(), (n1, n2) -> Math.min(n1, n2));
                                            } else {
                                                //check outputs for missing address records
                                                queryOutput.findOutputsByTransactionId(tx.getTransactionId()).stream()
                                                        .filter(to -> to.getAddressId() != 0)
                                                        .filter(to -> !queryAddress.findByAddressId(to.getAddressId()).isPresent())
                                                        .findAny().ifPresent(
                                                                to -> earliestBadBlockHeight.accumulateAndGet(to.getTransactionId(), (n1, n2) -> Math.min(n1, n2)));
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
                oLastBlockHeight = queryBlock.findLastHeight();
                if (!oLastBlockHeight.isPresent()) {
                    //if blocks table is empty, then truncate all tables except addresses
                    DBUtils.truncateTables(dbCon, updateBlock, updateTxn, updateInput, updateInputSpecial, updateOutput);
                } else {
                    deleteOrphanTransactions(oLastBlockHeight.get(), latestTransactions, db);
                    queryTransaction.getLastTransaction().ifPresent(tx -> deleteOrphanInsOuts(tx.getTransactionId(), db));
                }
            }
        } finally {
            execBlocks.shutdown();
            execTrans.shutdown();
            log.info("Pre-validation complete. Runtime: {}", Duration.ofNanos(start.getNanoTime()));
        }
    }

    /**
     * Delete orphan records from transaction table. Transaction records
     * considered to be orphan if their block height is larger than value in
     * lastBlockHeight argument.
     *
     * @param lastBlockHeight Last block height in "block" table
     * @param latestTransactions
     * @param db DB access object
     * @return total number of records deleted
     */
    private static int deleteOrphanTransactions(int lastBlockHeight, List<BtcTransaction> latestTransactions, DbAccess db) {
        AtomicInteger deletedTx = new AtomicInteger();
        latestTransactions.stream().filter(tx -> tx.getBlockHeight() > lastBlockHeight).forEach(tx -> {
            if (db.updateTransaction.delete(tx)) {
                deletedTx.incrementAndGet();
            }
        });
        if (deletedTx.get() != 0) {
            log.info("Deleted {} records from table {}", deletedTx.get(), db.updateTransaction.getTableName());
        }
        return deletedTx.get();
    }

    /**
     * Delete orphan records from input, input_special and output tables.
     * Records considered to be orphan if their transaction ID is larger than
     * value in lastTransactionId argument.
     *
     * @param lastTransactionId Last transaction id in "transaction" table
     * @param db DB access object
     * @return total number of records deleted
     */
    private static int deleteOrphanInsOuts(int lastTransactionId, DbAccess db) {
        int result = 0;
        int deleted = db.updateInput.deleteAllAboveTransactionId(lastTransactionId);
        if (deleted != 0) {
            log.info("Deleted {} orphan records from table {}", deleted, db.updateInput.getTableName());
            result += deleted;
        }
        deleted = db.updateInputSpecial.deleteAllAboveTransactionId(lastTransactionId);
        if (deleted != 0) {
            log.info("Deleted {} orphan records from table {}", deleted, db.updateInputSpecial.getTableName());
            result += deleted;
        }
        deleted = db.updateOutput.deleteAllAboveTransactionId(lastTransactionId);
        if (deleted != 0) {
            log.info("Deleted {} orphan records from table {}", deleted, db.updateOutput.getTableName());
            result += deleted;
        }
        return result;
    }

    @AllArgsConstructor
    private static class DbAccess {

        private final DbUpdateBlock updateBlock;
        private final DbUpdateTransaction updateTransaction;
        private final DbUpdateInput updateInput;
        private final DbUpdateInputSpecial updateInputSpecial;
        private final DbUpdateOutput updateOutput;
    }
}
