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
package com.sliva.btc.scanner;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.facade.DbQueryTransaction;
import com.sliva.btc.scanner.db.DbUpdate;
import com.sliva.btc.scanner.db.facade.DbUpdateOutput;
import com.sliva.btc.scanner.db.model.OutputStatus;
import com.sliva.btc.scanner.util.BufferingAheadSupplier;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOption;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOptions;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildCmdArguments;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.ShutdownHook;
import com.sliva.btc.scanner.util.Utils;
import com.sliva.btc.scanner.util.Utils.NumberFile;
import static com.sliva.btc.scanner.util.Utils.getNumberSupplier;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunUpdateSpent {

    private static final int DEFAULT_START_TRANSACTION_ID = 0;
    private static final int DEFAULT_BATCH_SIZE = 200_000;
    private static final int DEFAULT_THREADS = 3;

    private static final CmdOptions CMD_OPTS = new CmdOptions().add(DBConnectionSupplier.class).add(DbUpdate.class);
    private static final CmdOption batchSizeOpt = buildOption(CMD_OPTS, null, "batch-size", true, "Number or transactions to process in a batch. Default: " + DEFAULT_BATCH_SIZE);
    private static final CmdOption startFromOpt = buildOption(CMD_OPTS, null, "start-from", true, "Start process from this transaction ID. Beside a number this parameter can be set to a file name that stores the numeric value updated on every batch");
    private static final CmdOption threadsOpt = buildOption(CMD_OPTS, null, "threads", true, "Number of threads. Default: " + DEFAULT_THREADS);

    private static final String SQL_QUERY_OUTPUTS
            = "SELECT O.transaction_id,O.pos,O.address_id,O.spent,I.in_transaction_id FROM output O"
            + " LEFT JOIN input I ON I.in_transaction_id=O.transaction_id AND I.in_pos=O.pos"
            + " WHERE (O.address_id <> 0 OR O.spent <> " + OutputStatus.UNSPENDABLE + ") AND O.transaction_id BETWEEN ? AND ?";

    private static ShutdownHook shutdownHook;

    private final DBConnectionSupplier dbCon;
    private final DBPreparedStatement psQueryOutputs;
    private final DbQueryTransaction dbQueryTransaction;
    private final int startTransactionId;
    private final int batchSize;
    private final NumberFile startFromFile;
    private final int threads;

    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws Exception {
        CmdArguments cmd = buildCmdArguments(args, Main.Command.update_spent.name(), "Update \"spent\" column for existing transactions in in DB", null, CMD_OPTS);
        shutdownHook = new ShutdownHook();
        log.info("START");
        try {
            new RunUpdateSpent(cmd).runProcess();
        } finally {
            log.info("FINISH");
            DbUpdate.printStats();
            shutdownHook.finished();
        }
    }

    public RunUpdateSpent(CmdArguments cmd) {
        startFromFile = new Utils.NumberFile(cmd.getOption(startFromOpt).orElse(Integer.toString(DEFAULT_START_TRANSACTION_ID)));
        startTransactionId = startFromFile.getNumber().intValue();
        batchSize = cmd.getOption(batchSizeOpt).map(Integer::parseInt).orElse(DEFAULT_BATCH_SIZE);
        threads = cmd.getOption(threadsOpt).map(Integer::parseInt).orElse(DEFAULT_THREADS);
        dbCon = new DBConnectionSupplier().checkTablesExist("input", "output");
        psQueryOutputs = dbCon.prepareStatement(SQL_QUERY_OUTPUTS, "output.transaction_id", "input.in_transaction_id");
        dbQueryTransaction = new DbQueryTransaction(dbCon);
    }

    private void runProcess() throws SQLException {
        NumberFormat nf = NumberFormat.getIntegerInstance();
        int lastTxnId = dbQueryTransaction.getLastTransactionId().orElse(0);
        log.info("Run transactions from {} to {}", nf.format(startTransactionId), nf.format(lastTxnId));
        Supplier<Integer> batchNumberSupplier = getNumberSupplier(startTransactionId, batchSize, n -> n <= lastTxnId && !shutdownHook.isInterrupted());
        ExecutorService loadThreadpool = Executors.newFixedThreadPool(threads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("loadThread-%02d").build());
        StopWatch startTime = StopWatch.createStarted();
        try (DbUpdateOutput updateOutput = new DbUpdateOutput(dbCon)) {
            Supplier<CompletableFuture<DataSet>> preProcFeatureSupplier
                    = () -> CompletableFuture
                            .completedFuture(batchNumberSupplier.get())
                            .thenApplyAsync(n -> new DataSet(n,
                            psQueryOutputs
                                    .setParameters(p -> p.setInt(n).setInt(n + batchSize)).setFetchSize(batchSize * 5)
                                    .executeQueryToList(rs -> new Data(rs.getInt(1), rs.getShort(2), rs.getInt(3), rs.getByte(4), rs.getObject(5) != null))), loadThreadpool);
            BufferingAheadSupplier<CompletableFuture<DataSet>> bufferingSupplier = new BufferingAheadSupplier<>(preProcFeatureSupplier, threads * 2);
            while (!shutdownHook.isInterrupted()) {
                try {
                    DataSet dataSet = bufferingSupplier.get().get();
                    int numTxProcessed = dataSet.startTransactionId - startTransactionId;
                    log.info("Processing batch of outputs for transaction IDs between {} and {}. Size: {}, Speed: {} tx/sec",
                            nf.format(dataSet.startTransactionId), nf.format(dataSet.startTransactionId + batchSize), nf.format(dataSet.data.size()),
                            nf.format(numTxProcessed / Math.max(1, TimeUnit.NANOSECONDS.toSeconds(startTime.getNanoTime()))));
                    startFromFile.updateNumber(dataSet.startTransactionId);
                    dataSet.data.forEach(d -> {
                        if (!d.hasSpendingTransaction) {
                            if (d.addressId != 0 && d.spent != OutputStatus.UNSPENT) {
                                updateOutput.updateSpent(d.transactionId, d.pos, OutputStatus.UNSPENT);
                            } else if (d.addressId == 0 && d.spent < OutputStatus.UNSPENDABLE) {
                                updateOutput.updateSpent(d.transactionId, d.pos, OutputStatus.UNSPENDABLE);
                            }
                        } else if (d.spent != OutputStatus.SPENT) {
                            updateOutput.updateSpent(d.transactionId, d.pos, OutputStatus.SPENT);
                        }
                    });
                } catch (InterruptedException | ExecutionException | NoSuchElementException ex) {
                    log.info("MAIN: No More Elements - Exiting the loop. ({}:{})", ex.getClass().getSimpleName(), ex.getMessage());
                    break;
                }
            }
        }
    }

    @AllArgsConstructor
    private static class DataSet {

        private final int startTransactionId;
        private final Collection<Data> data;
    }

    @AllArgsConstructor
    private static class Data {

        private final int transactionId;
        private final short pos;
        private final int addressId;
        private final byte spent;
        private final boolean hasSpendingTransaction;
    }
}
