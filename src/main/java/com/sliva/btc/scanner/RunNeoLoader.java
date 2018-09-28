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
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;

/**
 *
 * @author whost
 */
@Slf4j
public class RunNeoLoader {

    private static final int DEFAULT_TXN_THREADS = 10;
    private static final int DEFAULT_START_TRANSACTION_ID = 1;
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-neo4j2-stop";
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
//    private final ExecutorService execNeo;
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
        new RunNeoLoader(cmd).run();
    }

    public RunNeoLoader(CommandLine cmd) {
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
        endTransaction = queryTransaction.getLastTransactionId();
        log.debug("startTransaction: {}, endTransaction: {}", startTransaction, endTransaction);
    }

    private void runLoop(NeoQueries neoQueries) throws Exception {
        Future<PrepData> prepFutureNext = prepareDataFuture(startTransaction);
        int nBatchesProcessed = 0;
        long s = System.currentTimeMillis();
        for (int i = startTransaction; i < endTransaction; i += batchSize) {
            if (stopFile.exists()) {
                log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                break;
            }
            Future<PrepData> prepFuture = prepFutureNext;
            prepFutureNext = prepareDataFuture(i + batchSize);
            uploadDataToNeoDB(neoQueries, prepFuture.get());
            nBatchesProcessed++;
            long runtime = System.currentTimeMillis() - s;
            log.debug("Loop#{}: Speed: {} tx/sec", nBatchesProcessed, (nBatchesProcessed * batchSize * 1000L / runtime));
        }
    }

    private Future<PrepData> prepareDataFuture(int start) {
        return execPrepareFiles.submit(() -> prepareData(start));
    }

    private PrepData prepareData(int startTransactionId) {
        int endTrasnactionId = startTransactionId + batchSize - 1;
        log.debug("prepareData [{} - {}] STARTED", startTransactionId, endTrasnactionId);
        long s = System.currentTimeMillis();
        PrepData data = new PrepData(startTransactionId, endTrasnactionId, UUID.randomUUID().toString());
        try {
            try {
                List<BtcTransaction> txList = queryTransaction.getTxnsRangle(startTransactionId, endTrasnactionId);
                execProcessTransactions.invokeAll(txList.stream().map((t) -> {
                    return (Callable<Void>) () -> {
                        try {
                            Map<String, Object> tx = new HashMap<>();
                            data.data.add(tx);
                            tx.put("id", t.getTransactionId());
                            tx.put("txid", t.getTxid());
                            tx.put("block", t.getBlockHeight());
                            tx.put("nInputs", t.getNInputs());
                            tx.put("nOutputs", t.getNOutputs());
                            Collection<Map<String, Object>> inputs = Collections.synchronizedCollection(new ArrayList<>());
                            tx.put("inputs", inputs);
                            queryInput.getInputs(t.getTransactionId()).forEach(i -> {
                                Map<String, Object> input = new HashMap<>();
                                inputs.add(input);
                                input.put("id", i.getInTransactionId() + ":" + i.getInPos());
                                input.put("pos", i.getPos());
                            });
                            Collection<Map<String, Object>> outputs = Collections.synchronizedCollection(new ArrayList<>());
                            tx.put("outputs", outputs);
                            queryOutput.getOutputs(t.getTransactionId()).forEach(o -> {
                                DbAddress adr = o.getAddressId() == 0 ? null : new DbAddress(blockProvider, o.getAddressId(), null, -1);
                                Map<String, Object> output = new HashMap<>();
                                outputs.add(output);
                                output.put("id", t.getTransactionId() + ":" + o.getPos());
                                output.put("pos", o.getPos());
                                output.put("address", adr == null ? "Undefined" : adr.getName());
                                output.put("amount", new BigDecimal(o.getAmount()).movePointLeft(8).doubleValue());
                                if (adr != null && adr.getWalletId() > 0) {
                                    DbWallet w = new DbWallet(blockProvider, adr.getWalletId(), null, null);
                                    String walletName = StringUtils.defaultString(w.getName(), Integer.toString(adr.getWalletId())).replaceAll("\"", "\"\"");
                                    Map<String, Object> wallet = new HashMap<>();
                                    wallet.put("id", adr.getWalletId());
                                    wallet.put("name", walletName);
                                    output.put("wallets", Collections.singleton(wallet));
                                }
                            });
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    };
                }).collect(Collectors.toList()));
            } finally {
                log.debug("prepareData [{} - {}]: FINISHED. Runtime: {} msec.", startTransactionId, endTrasnactionId, (System.currentTimeMillis() - s));
            }
        } catch (InterruptedException | SQLException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    @SuppressWarnings("UseSpecificCatch")
    private void uploadDataToNeoDB(NeoQueries neoQueries, PrepData data) throws Exception {
        log.debug("uploadDataToNeoDB [{} - {}] STARTED", data.startTransactionId, data.endTrasnactionId);
        long s = System.currentTimeMillis();
        try (Transaction t = neoQueries.beginTransaction()) {
            StatementResult sr = t.run(
                    "UNWIND {param} AS tx"
                    + " CREATE (t:Transaction {id:tx.id,hash:tx.hash,block:tx.block,nInputs:tx.nInputs,nOutputs:tx.nOutputs})"
                    + " FOREACH (outp IN tx.outputs |"
                    + " CREATE (o:Output{id:outp.id,address:outp.address,amount:outp.amount})<-[:output {pos:outp.pos}]-(t)"
                    + " FOREACH (wal IN outp.wallets |"
                    + " MERGE (w:Wallet{id:wal.id}) ON CREATE SET w.name=wal.name"
                    + " CREATE (o)-[:wallet]->(w)))"
                    + " WITH t, tx.inputs as inputs"
                    + " UNWIND inputs AS inp"
                    + " MATCH (o:Output {id:inp.id}) CREATE (o)-[oi:input {pos:inp.pos}]->(t)"
                    + " RETURN count(t),count(o),count(oi),count(inputs)",
                    Values.parameters("param", data.data));
            NeoQueries.logOutput(sr, "Output");
            t.commitAsync().thenRun(() -> log.debug("uploadDataToNeoDB [{} - {}]: Transaction committed", data.startTransactionId, data.endTrasnactionId));
        }
        log.debug("uploadDataToNeoDB [{} - {}] FINISHED. Runtime: {} msec.",
                data.startTransactionId, data.endTrasnactionId, (System.currentTimeMillis() - s));
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
        DBConnection.addOptions(options);
        NeoConnection.addOptions(options);
        return options;

    }

    private class PrepData {

        private final int startTransactionId;
        private final int endTrasnactionId;
        private final Collection<Map<String, Object>> data;

        PrepData(int startTransactionId, int endTrasnactionId, String uuid) {
            this.startTransactionId = startTransactionId;
            this.endTrasnactionId = endTrasnactionId;
            this.data = Collections.synchronizedCollection(new ArrayList<>(batchSize));
        }
    }
}
