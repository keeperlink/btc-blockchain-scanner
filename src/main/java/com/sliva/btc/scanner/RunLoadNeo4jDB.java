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
import com.sliva.btc.scanner.db.DbQueryAddress;
import com.sliva.btc.scanner.db.DbQueryAddressCombo;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryOutput;
import com.sliva.btc.scanner.db.DbQueryTransaction;
import com.sliva.btc.scanner.src.DbAddress;
import com.sliva.btc.scanner.src.DbBlockProvider;
import com.sliva.btc.scanner.src.DbWallet;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

/**
 *
 * @author whost
 */
@Slf4j
public class RunLoadNeo4jDB {

    private static final File uploadDir = new File("/tools/neo4j-community-3.4.7/import");
    private static final int DEFAULT_START_TRANSACTION_ID = 1;
    private static final int DEFAULT_BATCH_SIZE = 100_000;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-neo4j-stop";
    private final Driver driver;
    private final DBConnection dbCon;
//    private final DbQueryAddress queryAddress;
    private final DbQueryTransaction queryTransaction;
    private final DbQueryInput queryInput;
    private final DbQueryOutput queryOutput;
    private final DbBlockProvider blockProvider;
    private final Utils.NumberFile startFromFile;
    private final int batchSize;
    private final File stopFile;

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
        new RunLoadNeo4jDB(cmd).runProcess();
    }

    public RunLoadNeo4jDB(CommandLine cmd) {
        DBConnection.applyArguments(cmd);

        startFromFile = new Utils.NumberFile(cmd.getOptionValue("start-from", Integer.toString(DEFAULT_START_TRANSACTION_ID)));
        batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", Integer.toString(DEFAULT_BATCH_SIZE)));
        stopFile = new File(cmd.getOptionValue("stop-file", DEFAULT_STOP_FILE_NAME));
        Config.ConfigBuilder configBuilder = Config.build();
        driver = GraphDatabase.driver("bolt://127.0.0.1:7687", AuthTokens.basic("neo4j", "password"), configBuilder.toConfig());
        dbCon = new DBConnection();
//        queryAddress = new DbQueryAddressCombo(dbCon);
        queryTransaction = new DbQueryTransaction(dbCon);
        blockProvider = new DbBlockProvider(dbCon);
        queryInput = new DbQueryInput(dbCon);
        queryOutput = new DbQueryOutput(dbCon);
    }

    private void runProcess() throws Exception {
        final ExecutorService exec = Executors.newFixedThreadPool(2);
        try (Session session = driver.session()) {
            log.debug("Cleaning...");
            Utils.logRuntime("Cleaned", () -> {
//                session.run("MATCH (n:Transaction) DETACH DELETE n");
//                session.run("MATCH (n:Output) DETACH DELETE n");
//                session.run("MATCH (n:Wallet) DETACH DELETE n");
                session.run("CREATE CONSTRAINT ON (n:Transaction) ASSERT n.id IS UNIQUE");
                session.run("CREATE CONSTRAINT ON (n:Transaction) ASSERT n.hash IS UNIQUE");
                session.run("CREATE CONSTRAINT ON (n:Output) ASSERT n.id IS UNIQUE");
                session.run("CREATE CONSTRAINT ON (n:Wallet) ASSERT n.id IS UNIQUE");
            });
            final int startTransaction = startFromFile.getNumber().intValue();
            final int endTransaction = queryTransaction.getLastTransactionId();
            CompletableFuture<PrepFiles> prepFutureNext = CompletableFuture.supplyAsync(() -> {
                return processBatchPrepareFiles(startTransaction, startTransaction + batchSize - 1);
            }, exec);
            for (int i = startTransaction; i < endTransaction; i += batchSize) {
                if (stopFile.exists()) {
                    log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                    stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                    break;
                }
                CompletableFuture<PrepFiles> prepFuture = prepFutureNext;
                final int nextStart = i + batchSize;
                prepFutureNext = CompletableFuture.supplyAsync(() -> {
                    return processBatchPrepareFiles(nextStart, nextStart + batchSize - 1);
                }, exec);
                processBatchLoadFiles(session, prepFuture.get());
            }
        } finally {
            exec.shutdown();
            driver.close();
        }
    }

    @SuppressWarnings("UseSpecificCatch")
    private PrepFiles processBatchPrepareFiles(int startTransactionId, int endTrasnactionId) {
        log.debug("processBatchPrepareFiles [{} - {}] STARTED", startTransactionId, endTrasnactionId);
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
                queryTransaction.getTxnsRangle(startTransactionId, endTrasnactionId).parallelStream().forEach(t -> {
                    try {
                        synchronized (txnWriter) {
                            txnWriter.println(t.getTransactionId() + "," + t.getTxid() + "," + t.getBlockHeight() + "," + t.getNInputs() + "," + t.getNOutputs());
                        }
                        queryInput.getInputs(t.getTransactionId()).forEach(i -> {
                            synchronized (inWriter) {
                                int itid = i.getTransactionId();
                                int ipos = i.getPos();
                                int otid = i.getInTransactionId();
                                int opos = i.getInPos();
                                inWriter.println(itid + "," + ipos + "," + otid + "," + opos);
                            }
                        });
                        queryOutput.getOutputs(t.getTransactionId()).forEach(o -> {
                            DbAddress adr = o.getAddressId() == 0 ? null : new DbAddress(blockProvider, o.getAddressId(), null, -1);
                            String address = adr == null ? "Undefined" : adr.getName();
                            synchronized (outWriter) {
                                outWriter.println(o.getTransactionId() + "," + o.getPos() + ",\"" + address + "\","
                                        + new BigDecimal(o.getAmount()).movePointLeft(8));
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
                });
            } finally {
                log.debug("processBatchPrepareFiles [{} - {}]: FINISHED. Runtime: {} msec.", startTransactionId, endTrasnactionId, (System.currentTimeMillis() - s));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
//            txnFile.delete();
        }
        return f;
    }

    @SuppressWarnings("UseSpecificCatch")
    private void processBatchLoadFiles(Session session, PrepFiles f) throws Exception {
        log.debug("processBatchLoadFiles [{} - {}] STARTED", f.startTransactionId, f.endTrasnactionId);
        long s = System.currentTimeMillis();
        String txQuery = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:/" + f.txnFile.getName() + "\" as scv"
                + " CREATE (t:Transaction{id:scv.id,hash:scv.hash,block:scv.block,nInputs:scv.nInputs,nOutputs:scv.nOutputs})";
        Utils.logRuntime(txQuery, () -> session.run(txQuery));
        String outQuery = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:/" + f.outFile.getName() + "\" as scv"
                + " MATCH (t:Transaction {id:scv.transactionId})"
                + " CREATE (o:Output{id:scv.transactionId+':'+scv.pos,address:scv.address,amount:scv.amount})<-[:output {pos:scv.pos}]-(t)";
        Utils.logRuntime(outQuery, () -> session.run(outQuery));
        String inQuery = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:/" + f.inFile.getName() + "\" as scv"
                + " MATCH (t:Transaction {id:scv.transactionId}),(o:Output {id:scv.inTransactionId+':'+scv.inPos})"
                + " CREATE (o)-[:input {pos:scv.pos}]->(t)";
        Utils.logRuntime(inQuery, () -> session.run(inQuery));
        String wQuery = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:/" + f.wFile.getName() + "\" as scv"
                + " MATCH (o:Output {id:scv.transactionId+':'+scv.pos})"
                + " MERGE (w:Wallet{id:scv.walletId}) ON CREATE SET w.name=scv.walletName"
                + " CREATE (o)-[:wallet]->(w)";
        Utils.logRuntime(wQuery, () -> session.run(wQuery));
        log.debug("processBatchLoadFiles [{} - {}] FINISHED. Runtime: {} msec.",
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
        DBConnection.addOptions(options);
        return options;
    }

    @Getter
    private static class PrepFiles {

        private final int startTransactionId;
        private final int endTrasnactionId;
        private final File txnFile;
        private final File outFile;
        private final File inFile;
        private final File wFile;

        public PrepFiles(int startTransactionId, int endTrasnactionId, String uuid) {
            this.startTransactionId = startTransactionId;
            this.endTrasnactionId = endTrasnactionId;
            txnFile = new File(uploadDir, uuid + "-txn.scv");
            outFile = new File(uploadDir, uuid + "-out.scv");
            inFile = new File(uploadDir, uuid + "-in.scv");
            wFile = new File(uploadDir, uuid + "-w.scv");
        }

    }
}
