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

import com.sliva.btc.scanner.db.DBConnection;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryOutput;
import com.sliva.btc.scanner.db.DbQueryTransaction;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.neo4j.NeoQueries;
import com.sliva.btc.scanner.neo4j.NeoConnection;
import com.sliva.btc.scanner.src.DbAddress;
import com.sliva.btc.scanner.src.DbBlockProvider;
import com.sliva.btc.scanner.src.DbWallet;
import com.sliva.btc.scanner.util.CustomThreadFactory;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

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
    private final DBConnection dbCon;
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
    private final ExecutorService execNeo;
    private File uploadDir;
    private final NeoConnection neoConn;

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
        RunLoadNeo4jDB main = new RunLoadNeo4jDB(cmd);
        try {
            main.runProcess();
        } finally {
            main.shutdown();
        }
    }

    public RunLoadNeo4jDB(CommandLine cmd) {
        DBConnection.applyArguments(cmd);
        NeoConnection.applyArguments(cmd);
        startFromFile = new Utils.NumberFile(cmd.getOptionValue("start-from", Integer.toString(DEFAULT_START_TRANSACTION_ID)));
        batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", Integer.toString(DEFAULT_BATCH_SIZE)));
        stopFile = new File(cmd.getOptionValue("stop-file", DEFAULT_STOP_FILE_NAME));
        cleanup = false;
        int nTxnThreads = Integer.parseInt(cmd.getOptionValue("threads", Integer.toString(DEFAULT_TXN_THREADS)));
        dbCon = new DBConnection();
//        queryAddress = new DbQueryAddressCombo(dbCon);
        queryTransaction = new DbQueryTransaction(dbCon);
        blockProvider = new DbBlockProvider(dbCon);
        queryInput = new DbQueryInput(dbCon);
        queryOutput = new DbQueryOutput(dbCon);
        execPrepareFiles = Executors.newFixedThreadPool(2, new CustomThreadFactory("PrepareFiles", true));
        execProcessTransactions = Executors.newFixedThreadPool(nTxnThreads, new CustomThreadFactory("ProcessTxn", true));
        execNeo = Executors.newFixedThreadPool(2, new CustomThreadFactory("NeoUpload", true));
        neoConn = new NeoConnection();
    }

    private void shutdown() {
        log.debug("Initiating shutdown");
        execPrepareFiles.shutdown();
        execProcessTransactions.shutdown();
        execNeo.shutdown();
        neoConn.close();
        log.debug("Shutdown complete.");
    }

    private void runProcess() throws Exception {

        Executors.defaultThreadFactory();
        try (NeoQueries neoQueries = new NeoQueries(neoConn)) {
            String importDir = neoQueries.getImportDirectory();
            log.debug("Neo4j import directory: " + importDir);
            if (importDir == null) {
                throw new IllegalStateException("Cannot determine Neo4j import directory");
            }
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
            final int lastTransactionId = neoQueries.getLastTransactionId();
            final int startTransaction = lastTransactionId + 1;//startFromFile.getNumber().intValue();
            log.debug("Getting Neo DB state...");
            final int endTransaction = queryTransaction.getLastTransactionId();
            log.debug("startTransaction: {}, endTransaction: {}", startTransaction, endTransaction);
            Future<PrepFiles> prepFutureNext = execPrepareFiles.submit(() -> prepareFiles(startTransaction, startTransaction + batchSize - 1));
            new CleanupFiles().start();
            int nBatchesProcessed = 0;
            long s = System.currentTimeMillis();
            for (int i = startTransaction; i < endTransaction; i += batchSize) {
                if (stopFile.exists()) {
                    log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                    stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                    break;
                }
                Future<PrepFiles> prepFuture = prepFutureNext;
                final int nextStart = i + batchSize;
                prepFutureNext = execPrepareFiles.submit(() -> prepareFiles(nextStart, nextStart + batchSize - 1));
                uploadFilesToNeoDB(neoQueries, prepFuture.get());
                nBatchesProcessed++;
                long runtime = System.currentTimeMillis() - s;
                log.debug("Loop#{}: Speed: {} tx/sec", nBatchesProcessed, (nBatchesProcessed * batchSize * 1000L / runtime));
            }
        }
    }

    @SuppressWarnings("UseSpecificCatch")
    private PrepFiles prepareFiles(int startTransactionId, int endTrasnactionId) {
        log.debug("prepareFiles [{} - {}] STARTED", startTransactionId, endTrasnactionId);
        long s = System.currentTimeMillis();
        PrepFiles f = new PrepFiles(startTransactionId, endTrasnactionId, UUID.randomUUID().toString());
//        log.debug("txnFile: {}", f.txnFile.toURI());
//        txnFile.deleteOnExit();
//        outFile.deleteOnExit();
//        inFile.deleteOnExit();
//        wFile.deleteOnExit();
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
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    };
                }).collect(Collectors.toList()));
            } finally {
                log.debug("prepareFiles [{} - {}]: FINISHED. Runtime: {} msec.", startTransactionId, endTrasnactionId, (System.currentTimeMillis() - s));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
//            txnFile.delete();
        }
        return f;
    }

    @SuppressWarnings("UseSpecificCatch")
    private void uploadFilesToNeoDB(NeoQueries neoQueries, PrepFiles f) throws Exception {
        log.debug("uploadFilesToNeoDB [{} - {}] STARTED", f.startTransactionId, f.endTrasnactionId);
        long s = System.currentTimeMillis();
        neoQueries.uploadFile(f.txnFile, false,
                "CREATE (t:Transaction{id:toInteger(v.id),hash:v.hash,block:toInteger(v.block),nInputs:toInteger(v.nInputs),nOutputs:toInteger(v.nOutputs)})"
                + " RETURN count(v),count(t)");
        neoQueries.uploadFile(f.outFile, false,
                "MATCH (t:Transaction {id:toInteger(v.transactionId)})"
                + " CREATE (o:Output{id:v.transactionId+':'+v.pos,address:v.address,amount:v.amount})<-[p:output {pos:toInteger(v.pos)}]-(t)"
                + " RETURN count(v),count(t),count(o),count(p)");
        neoQueries.uploadFile(f.inFile, false,
                "MATCH (t:Transaction {id:toInteger(v.transactionId)}),(o:Output {id:v.inTransactionId+':'+v.inPos})"
                + " CREATE (o)-[p:input {pos:toInteger(v.pos)}]->(t)"
                + " RETURN count(v),count(t),count(o),count(p)");
        neoQueries.uploadFile(f.wFile, false,
                "MATCH (o:Output {id:v.transactionId+':'+v.pos})"
                + " MERGE (w:Wallet{id:toInteger(v.walletId)}) ON CREATE SET w.name=v.walletName"
                + " CREATE (o)-[p:wallet]->(w)"
                + " RETURN count(v),count(o),count(w),count(p)");
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
        options.addOption(null, "start-from", true, "Start process from this transaction ID. Beside a number this parameter can be set to a file name that stores the numeric value updated on every batch");
        options.addOption(null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end. Default: " + DEFAULT_STOP_FILE_NAME);
        options.addOption("t", "threads", true, "Number of threads to run. Default is " + DEFAULT_TXN_THREADS + ". To disable parallel threading set value to 0");
        DBConnection.addOptions(options);
        NeoConnection.addOptions(options);
        return options;

    }

    @Getter
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

        private static final long TTL_MSEC = 10 * 60 * 1000L;
        private static final long CHECK_PERIOD_MSEC = 30 * 1000L;

        public CleanupFiles() {
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
