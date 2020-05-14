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
import com.sliva.btc.scanner.db.fasade.DbQueryOutput;
import com.sliva.btc.scanner.db.fasade.DbQueryOutput.OutputAddressWallet;
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.neo4j.NeoConnection;
import com.sliva.btc.scanner.neo4j.NeoQueries;
import com.sliva.btc.scanner.neo4j.NeoQueries.OutputWithWallet;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 *
 * @author whost
 */
@Slf4j
public class RunNeoUpdateWallets {

    private static final boolean DEFAULT_SAFE_RUN = false;
    private static final boolean DEFAULT_CLEANUP = false;
    private static final int DEFAULT_TXN_THREADS = 5;
    private static final int DEFAULT_START_TRANSACTION_ID = 1;
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-neoUpdateWallets-stop";
    private final DBConnectionSupplier dbCon;
//    private final DbQueryAddress queryAddress;
    private final DbQueryOutput queryOutput;
//    private final DbBlockProvider blockProvider;
    private final Utils.NumberFile startFromFile;
    private final int batchSize;
    private final File stopFile;
//    private final ExecutorService execPrepareFiles;
//    private final ExecutorService execProcessTransactions;
//    private final ExecutorService execNeo;
    private final ExecutorService exec = Executors.newFixedThreadPool(3, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Process-%d").build());
    ;
    private final NeoConnection neoConn;
    private final int recordsBack;
    private int startTransaction;
    private int endTransaction;
//    private final LoadingCache<Integer, CAddress> addressCache;
//    private final LoadingCache<Integer, Optional<String>> walletCache;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(prepOptions(), args);
        if (cmd.hasOption('h')) {
            printHelpAndExit();
        }
        new RunNeoUpdateWallets(cmd).run();
    }

    public RunNeoUpdateWallets(CommandLine cmd) {
        DBConnectionSupplier.applyArguments(cmd);
        NeoConnection.applyArguments(cmd);
        startFromFile = cmd.hasOption("start-from") ? new Utils.NumberFile(cmd.getOptionValue("start-from", Integer.toString(DEFAULT_START_TRANSACTION_ID))) : null;
        batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", Integer.toString(DEFAULT_BATCH_SIZE)));
        stopFile = new File(cmd.getOptionValue("stop-file", DEFAULT_STOP_FILE_NAME));
        recordsBack = cmd.hasOption("records-back") ? Integer.parseInt(cmd.getOptionValue("records-back")) : 0;
//        int nTxnThreads = Integer.parseInt(cmd.getOptionValue("threads", Integer.toString(DEFAULT_TXN_THREADS)));
        dbCon = new DBConnectionSupplier();
//        queryAddress = new DbQueryAddressCombo(dbCon);
//        blockProvider = new DbBlockProvider(dbCon);
        queryOutput = new DbQueryOutput(dbCon);
//        execPrepareFiles = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("PrepareFiles-%d").build());
//        execProcessTransactions = Executors.newFixedThreadPool(nTxnThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ProcessTxn-%d").build());
//        execNeo = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setDaemon(false).setNameFormat("NeoUpload-%d").build());
        neoConn = new NeoConnection();
//        addressCache = CacheBuilder.newBuilder()
//                .concurrencyLevel(nTxnThreads)
//                .maximumSize(1_000_000)
//                .recordStats()
//                .build(
//                        new CacheLoader<Integer, CAddress>() {
//                    @Override
//                    public CAddress load(Integer addressId) {
//                        DbAddress a = new DbAddress(blockProvider, addressId, null, -1);
//                        return new CAddress(a.getName(), a.getWalletId());
//                    }
//                });
//        walletCache = CacheBuilder.newBuilder()
//                .concurrencyLevel(nTxnThreads)
//                .maximumSize(1_000_000)
//                .recordStats()
//                .build(
//                        new CacheLoader<Integer, Optional<String>>() {
//                    @Override
//                    public Optional<String> load(Integer walletId) {
//                        DbWallet w = new DbWallet(blockProvider, walletId, null, null);
//                        return Optional.fromNullable(w.getName());
//                    }
//                });
    }

    private void shutdown() {
        log.debug("Initiating shutdown");
//        execPrepareFiles.shutdown();
//        execProcessTransactions.shutdown();
//        execNeo.shutdown();
        exec.shutdown();
        neoConn.close();
        log.debug("Shutdown complete.");
    }

    private void run() throws Exception {
        try (NeoQueries neoQueries = new NeoQueries(neoConn)) {
            init(neoQueries);
            runLoop(neoQueries);
        } finally {
            shutdown();
        }
    }

    private void init(NeoQueries neoQueries) throws SQLException {
        log.debug("Getting Neo DB state...");
        final int lastTransactionId = neoQueries.getLastTransactionId();
        if (startFromFile != null && startFromFile.getNumber() != null) {
            startTransaction = startFromFile.getNumber().intValue();
        } else {
            startTransaction = lastTransactionId + 1 - recordsBack;
        }
        endTransaction = lastTransactionId;
        log.debug("startTransaction: {}, endTransaction: {}", startTransaction, endTransaction);
    }

    private void runLoop(NeoQueries neoQueries) throws Exception {
        int nBatchesProcessed = 0;
        long s = System.currentTimeMillis();
        for (int i = startTransaction; i < endTransaction; i += batchSize) {
            if (stopFile.exists()) {
                log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                break;
            }
            if (startFromFile != null) {
                startFromFile.updateNumber(i);
            }
            final int from = i, to = i + batchSize;
            Future<Collection<OutputAddressWallet>> sqlProc = exec.submit(() -> {
                final Collection<OutputAddressWallet> fromSQL = new ArrayList<>();
                Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal)
                        .forEach(t -> fromSQL.addAll(queryOutput.queryOutputsInTxnRange(from, to - 1, t)));
                return fromSQL;
            });
            Future<Collection<OutputWithWallet>> neoProc = exec.submit(() -> {
                return neoQueries.queryOutputsWithWallets(from, to);
            });
            final Collection<OutputAddressWallet> fromSQL = sqlProc.get();
            final Collection<OutputWithWallet> fromNeo = neoProc.get();
            exec.submit(() -> processResults(fromSQL, fromNeo, neoQueries));
            nBatchesProcessed++;
            long runtime = System.currentTimeMillis() - s;
            log.debug("Loop#{}: Speed: {} tx/sec.",
                    nBatchesProcessed, (nBatchesProcessed * batchSize * 1000L / runtime));
        }
    }

    private void processResults(Collection<OutputAddressWallet> fromSQL, Collection<OutputWithWallet> fromNeo, NeoQueries neoQueries) {
        long s = System.currentTimeMillis();
        final Map<InOutKey, OutputAddressWallet> dbMap = fromSQL.parallelStream().collect(Collectors
                .toMap(e -> new InOutKey(e.getTransactionId(), e.getPos()), Function.identity()));
        //final Set<OutputWithWallet> neoSet = new HashSet<>(fromNeo);
        final Set<InOutKey> found = new HashSet<>();
        fromNeo.forEach(neoData -> {
            InOutKey key = new InOutKey(neoData.getTransactionId(), neoData.getPos());
            //check for multiple output-->wallet relations
            if (found.contains(key)) {
                log.warn("Duplicate wallet relations found for Output: txID:{}, pos:{}, outputId={}",
                        key.getTransactionId(), key.getPos(), toOutputId(key.getTransactionId(), key.getPos()));
            }
            found.add(key);
            OutputAddressWallet dbData = dbMap.get(key);
            if (dbData == null) {
                log.info("//relations not in SQL DB - remove it from Neo. neoData={}", neoData);
                neoQueries.deleteOutputWalletRelation(neoData.getTransactionId(), neoData.getPos(), neoData.getWalletId());
            } else {
                if (dbData.getWalletId() != neoData.getWalletId()) {
                    log.info("//different relations - update relation in Neo. neoData={}, dbData={}", neoData, dbData);
                    neoQueries.deleteOutputWalletRelation(neoData.getTransactionId(), neoData.getPos(), neoData.getWalletId());
                    neoQueries.updateOutputWalletRelation(dbData.getTransactionId(), dbData.getPos(), dbData.getWalletId(), dbData.getWalletName());
                } else if (dbData.getWalletName() != null && !dbData.getWalletName().equals(neoData.getWalletName())) {
                    log.info("//Wallet name is different - update it in Neo. neoData={}, dbData={}", neoData, dbData);
                    neoQueries.updateOutputWalletRelation(dbData.getTransactionId(), dbData.getPos(), dbData.getWalletId(), dbData.getWalletName());
                }
                dbMap.remove(key);
            }
        });
        if (!dbMap.isEmpty()) {
            log.info("Missing relations: {}", dbMap.size());
            dbMap.forEach((k, v) -> neoQueries.updateOutputWalletRelation(v.getTransactionId(), v.getPos(), v.getWalletId(), v.getWalletName()));
        }
        log.debug("processResults: FINISHED. Runtime {} msec.", System.currentTimeMillis() - s);
    }

    private static long toOutputId(int transactionId, int pos) {
        return transactionId * 100_000L + pos;
    }

    private static void printHelpAndExit() {
        System.out.println("Available options:");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java <jar> " + Main.Command.update_neo_wallets + " [options]", prepOptions());
        System.exit(1);
    }

    private static Options prepOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        options.addOption(null, "safe-run", false, "Run in safe mode - check DB for existing records before adding new. Default: " + DEFAULT_SAFE_RUN);
        options.addOption(null, "cleanup", false, "Clean DB on start. Default: " + DEFAULT_CLEANUP);
        options.addOption(null, "batch-size", true, "Number or transactions to process in a batch. Default: " + DEFAULT_BATCH_SIZE);
        options.addOption(null, "start-from", true, "Start process from this transaction ID. Beside a number this parameter can be set to a file name that stores the numeric value updated on every batch");
        options.addOption(null, "records-back", true, "Check last number of trasnactions. Process will run in safe mode (--safe-run=true)");
        options.addOption(null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end. Default: " + DEFAULT_STOP_FILE_NAME);
        options.addOption(null, "threads", true, "Number of threads to run. Default is " + DEFAULT_TXN_THREADS + ". To disable parallel threading set value to 0");
        DBConnectionSupplier.addOptions(options);
        NeoConnection.addOptions(options);
        return options;

    }

    @Getter
    @AllArgsConstructor
    private static class CAddress {

        private final String name;
        private final int walletId;
    }

}
