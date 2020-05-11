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

import static com.google.common.base.Preconditions.checkState;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sliva.btc.scanner.Main;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryOutput;
import com.sliva.btc.scanner.db.DbQueryTransaction;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.neo4j.NeoConnection;
import com.sliva.btc.scanner.neo4j.NeoQueries;
import com.sliva.btc.scanner.src.DbAddress;
import com.sliva.btc.scanner.src.DbBlockProvider;
import com.sliva.btc.scanner.src.DbWallet;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author whost
 */
@Slf4j
public class RunLoadNeo4jDB {

    private static final String FILE_SUFFIX_TXN = "-txn.scv";
    private static final String FILE_SUFFIX_OUT = "-out.scv";
    private static final String FILE_SUFFIX_IN = "-in.scv";
    private static final String FILE_SUFFIX_WALLET = "-w.scv";
    private static final int DEFAULT_TXN_THREADS = 10;
    private static final int DEFAULT_START_TRANSACTION_ID = 1;
    private static final int DEFAULT_BATCH_SIZE = 100_000;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-neo4j-stop";
    private final DBConnectionSupplier dbCon;
//    private final DbQueryAddress queryAddress;
    private final DbQueryTransaction queryTransaction;
    private final DbQueryInput queryInput;
    private final DbQueryOutput queryOutput;
    private final DbBlockProvider blockProvider;
    private final Utils.NumberFile startFromFile;
    private final int batchSize;
    private final boolean cleanup;
    private final File stopFile;
    private final ExecutorService execPrepareFiles;
    private final ExecutorService execProcessTransactions;
//    private final ExecutorService execNeo;
    private File uploadDir;
    private final NeoConnection neoConn;
    private int startTransaction;
    private int endTransaction;

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
        new RunLoadNeo4jDB(cmd).run();
    }

    public RunLoadNeo4jDB(CommandLine cmd) {
        DBConnectionSupplier.applyArguments(cmd);
        NeoConnection.applyArguments(cmd);
        startFromFile = new Utils.NumberFile(cmd.getOptionValue("start-from", Integer.toString(DEFAULT_START_TRANSACTION_ID)));
        batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", Integer.toString(DEFAULT_BATCH_SIZE)));
        stopFile = new File(cmd.getOptionValue("stop-file", DEFAULT_STOP_FILE_NAME));
        cleanup = false;
        int nTxnThreads = Integer.parseInt(cmd.getOptionValue("threads", Integer.toString(DEFAULT_TXN_THREADS)));
        dbCon = new DBConnectionSupplier();
//        queryAddress = new DbQueryAddressCombo(dbCon);
        queryTransaction = new DbQueryTransaction(dbCon);
        blockProvider = new DbBlockProvider(dbCon);
        queryInput = new DbQueryInput(dbCon);
        queryOutput = new DbQueryOutput(dbCon);
        execPrepareFiles = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("PrepareFiles-%d").build());
        execProcessTransactions = Executors.newFixedThreadPool(nTxnThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ProcessTxn-%d").build());
//        execNeo = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setDaemon(false).setNameFormat("NeoUpload-%d").build());
        neoConn = new NeoConnection();
    }

    private void shutdown() {
        log.debug("Initiating shutdown");
        execPrepareFiles.shutdown();
        execProcessTransactions.shutdown();
//        execNeo.shutdown();
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
        String importDir = neoQueries.getImportDirectory();
        log.debug("Neo4j import directory: " + importDir);
        checkState(importDir != null, "Cannot determine Neo4j import directory");
        uploadDir = new File(importDir);
        if (cleanup) {
            log.debug("Cleaning...");
            Utils.logRuntime("Cleaned", () -> {
                neoQueries.run("MATCH (n:Transaction) DETACH DELETE n");
                neoQueries.run("MATCH (n:Output) DETACH DELETE n");
                neoQueries.run("MATCH (n:Wallet) DETACH DELETE n");
            });
        }
        neoQueries.run("CREATE CONSTRAINT ON (n:Transaction) ASSERT n.id IS UNIQUE");
        neoQueries.run("CREATE CONSTRAINT ON (n:Transaction) ASSERT n.hash IS UNIQUE");
        neoQueries.run("CREATE CONSTRAINT ON (n:Output) ASSERT n.id IS UNIQUE");
        neoQueries.run("CREATE CONSTRAINT ON (n:Wallet) ASSERT n.id IS UNIQUE");
        log.debug("Getting Neo DB state...");
        final int lastTransactionId = neoQueries.getLastTransactionId();
        startTransaction = lastTransactionId + 1;//startFromFile.getNumber().intValue();
        endTransaction = queryTransaction.getLastTransactionId().orElse(0);
        log.debug("startTransaction: {}, endTransaction: {}", startTransaction, endTransaction);
        new CleanupFiles().start();
    }

    private void runLoop(NeoQueries neoQueries) throws Exception {
        Future<PrepFiles> prepFutureNext = prepareFilesFuture(startTransaction);
        int nBatchesProcessed = 0;
        long s = System.currentTimeMillis();
        for (int i = startTransaction; i < endTransaction; i += batchSize) {
            if (stopFile.exists()) {
                log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                break;
            }
            Future<PrepFiles> prepFuture = prepFutureNext;
            prepFutureNext = prepareFilesFuture(i + batchSize);
            uploadFilesToNeoDB(neoQueries, prepFuture.get());
            nBatchesProcessed++;
            long runtime = System.currentTimeMillis() - s;
            log.debug("Loop#{}: Speed: {} tx/sec", nBatchesProcessed, (nBatchesProcessed * batchSize * 1000L / runtime));
        }
    }

    private Future<PrepFiles> prepareFilesFuture(int start) {
        return execPrepareFiles.submit(() -> prepareFiles(start));
    }

    private PrepFiles prepareFiles(int startTransactionId) {
        int endTrasnactionId = startTransactionId + batchSize - 1;
        log.debug("prepareFiles [{} - {}] STARTED", startTransactionId, endTrasnactionId);
        long s = System.currentTimeMillis();
        PrepFiles f = new PrepFiles(startTransactionId, endTrasnactionId, UUID.randomUUID().toString());
        try {
            try (PrintWriter txnWriter = new PrintWriter(f.txnFile);
                    PrintWriter outWriter = new PrintWriter(f.outFile);
                    PrintWriter inWriter = new PrintWriter(f.inFile);
                    PrintWriter wWriter = new PrintWriter(f.wFile)) {
                txnWriter.println("id,hash,block,nInputs,nOutputs");
                outWriter.println("transactionId,pos,address,amount");
                inWriter.println("transactionId,pos,inTransactionId,inPos");
                wWriter.println("transactionId,pos,walletId,walletName");
                List<BtcTransaction> txList = queryTransaction.getTxnsRangle(startTransactionId, endTrasnactionId);
                execProcessTransactions.invokeAll(txList.stream().map((t) -> {
                    return (Callable<Void>) () -> {
                        try {
                            synchronized (txnWriter) {
                                txnWriter.println(t.getTransactionId() + "," + t.getTxid() + "," + t.getBlockHeight() + "," + t.getNInputs() + "," + t.getNOutputs());
                            }
                            queryInput.getInputs(t.getTransactionId()).forEach(i -> {
                                synchronized (inWriter) {
                                    inWriter.println(i.getTransactionId() + "," + i.getPos() + "," + i.getInTransactionId() + "," + i.getInPos());
                                }
                            });
                            queryOutput.getOutputs(t.getTransactionId()).forEach(o -> {
                                DbAddress adr = o.getAddressId() == 0 ? null : new DbAddress(blockProvider, o.getAddressId(), null, -1);
                                String address = adr == null ? "Undefined" : adr.getName();
                                synchronized (outWriter) {
                                    BigDecimal amount = new BigDecimal(o.getAmount()).movePointLeft(8);
                                    outWriter.println(o.getTransactionId() + "," + o.getPos() + ",\"" + address + "\"," + amount);
                                }
                                int walletId = adr == null ? 0 : adr.getWalletId();
                                if (walletId > 0) {
                                    DbWallet wallet = new DbWallet(blockProvider, walletId, null, null);
                                    String walletName = StringUtils.defaultString(wallet.getName(), Integer.toString(walletId)).replaceAll("\"", "\"\"");
                                    synchronized (wWriter) {
                                        wWriter.println(o.getTransactionId() + "," + o.getPos() + "," + walletId + ",\"" + walletName + "\"");
                                    }
                                }
                            });
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    };
                }).collect(Collectors.toList()));
            } finally {
                log.debug("prepareFiles [{} - {}]: FINISHED. Runtime: {} msec.", startTransactionId, endTrasnactionId, (System.currentTimeMillis() - s));
            }
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
        return f;
    }

    @SuppressWarnings("UseSpecificCatch")
    private void uploadFilesToNeoDB(NeoQueries neoQueries, PrepFiles f) throws Exception {
        log.debug("uploadFilesToNeoDB [{} - {}] STARTED", f.startTransactionId, f.endTrasnactionId);
        long s = System.currentTimeMillis();
        neoQueries.uploadFile(f.txnFile, false,
                "CREATE (t:Transaction{id:toInteger(v.id),hash:v.hash,block:toInteger(v.block),nInputs:toInteger(v.nInputs),nOutputs:toInteger(v.nOutputs)})"
                + " RETURN count(v)");
        neoQueries.uploadFile(f.outFile, false,
                "MATCH (t:Transaction {id:toInteger(v.transactionId)})"
                + " CREATE (o:Output{id:v.transactionId+':'+v.pos,address:v.address,amount:v.amount})<-[:output {pos:toInteger(v.pos)}]-(t)"
                + " RETURN count(v)");
        neoQueries.uploadFile(f.inFile, false,
                "MATCH (t:Transaction {id:toInteger(v.transactionId)}),(o:Output {id:v.inTransactionId+':'+v.inPos})"
                + " CREATE (o)-[:input {pos:toInteger(v.pos)}]->(t)"
                + " RETURN count(v)");
        neoQueries.uploadFile(f.wFile, false,
                "MATCH (o:Output {id:v.transactionId+':'+v.pos})"
                + " MERGE (w:Wallet{id:toInteger(v.walletId)}) ON CREATE SET w.name=v.walletName"
                + " CREATE (o)-[:wallet]->(w)"
                + " RETURN count(v)");
        log.debug("uploadFilesToNeoDB [{} - {}] FINISHED. Runtime: {} msec.",
                f.startTransactionId, f.endTrasnactionId, (System.currentTimeMillis() - s));
    }

    private static void printHelpAndExit() {
        System.out.println("Available options:");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java <jar> " + Main.Command.load_neo4j + " [options]", prepOptions());
        System.exit(1);
    }

    private static Options prepOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        options.addOption(null, "batch-size", true, "Number or transactions to process in a batch. Default: " + DEFAULT_BATCH_SIZE);
        options.addOption(null, "start-from", true, "Start process from this transaction ID. Beside a number this parameter can be set to a file name that stores the numeric value updated on every batch");
        options.addOption(null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end. Default: " + DEFAULT_STOP_FILE_NAME);
        options.addOption("t", "threads", true, "Number of threads to run. Default is " + DEFAULT_TXN_THREADS + ". To disable parallel threading set value to 0");
        DBConnectionSupplier.addOptions(options);
        NeoConnection.addOptions(options);
        return options;

    }

    private class PrepFiles {

        private final int startTransactionId;
        private final int endTrasnactionId;
        private final File txnFile;
        private final File outFile;
        private final File inFile;
        private final File wFile;

        PrepFiles(int startTransactionId, int endTrasnactionId, String uuid) {
            this.startTransactionId = startTransactionId;
            this.endTrasnactionId = endTrasnactionId;
            txnFile = new File(uploadDir, uuid + FILE_SUFFIX_TXN);
            outFile = new File(uploadDir, uuid + FILE_SUFFIX_OUT);
            inFile = new File(uploadDir, uuid + FILE_SUFFIX_IN);
            wFile = new File(uploadDir, uuid + FILE_SUFFIX_WALLET);
        }
    }

    private class CleanupFiles extends Thread {

        private static final long TTL_MSEC = 5 * 60 * 1000L;
        private static final long CHECK_PERIOD_MSEC = 30 * 1000L;

        private CleanupFiles() {
            super("CleanupFiles");
            setDaemon(true);
        }

        @Override
        public void run() {
            log.debug("CleanupFiles: START");
            for (;;) {
                try {
                    cleanup();
                } catch (Exception e) {
                    log.error(null, e);
                } finally {
                    Utils.sleep(CHECK_PERIOD_MSEC);
                }
            }
        }

        private void cleanup() {
            if (uploadDir != null) {
                for (File f : uploadDir.listFiles()) {
                    String name = f.getName();
                    if (name.endsWith(FILE_SUFFIX_TXN) || name.endsWith(FILE_SUFFIX_OUT) || name.endsWith(FILE_SUFFIX_IN) || name.endsWith(FILE_SUFFIX_WALLET)) {
                        if (System.currentTimeMillis() - f.lastModified() > TTL_MSEC) {
                            f.delete();
                        }
                    }
                }
            }
        }
    }
}
