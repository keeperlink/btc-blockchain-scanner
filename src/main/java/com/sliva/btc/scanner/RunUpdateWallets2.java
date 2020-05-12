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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.DbUpdateWallet;
import com.sliva.btc.scanner.db.DbQueries;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryTransaction;
import com.sliva.btc.scanner.db.DbQueryWallet;
import com.sliva.btc.scanner.db.DbUpdateAddress;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
public class RunUpdateWallets2 {

    private static final int DEFAULT_TXN_THREADS = 20;
    private static final int DEFAULT_FIRST_TRANSACTION = 1;
    private static final int DEFAULT_BATCH_SIZE = 20000;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-update-wallet-stop";
    private static final String SQL_UPDATE_ADDRESS_WALLET
            = "UPDATE address_table_name SET wallet_id=? WHERE wallet_id=?";

    private final File stopFile;
    private final DBConnectionSupplier conn;
    private final DbQueryTransaction queryTransaction;
    private final DbQueryInput queryInput;
    private final DbQueryWallet queryWallet;
    private final DbQueries dbQueries;
    private final Collection<DBPreparedStatement> psUpdateAddressWalletPerTable;
    private final int firstTransaction;
    private final int batchSize;
    private final int txnThreads;
    private final Set<Integer> unusedWallets = new HashSet<>();
    private final ExecutorService execAddressQueries = Executors.newFixedThreadPool((int) Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).count());
    private final ExecutorService execTransactionThreads;
    private final Utils.NumberFile startFromFile;

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(prepOptions(), args);
        if (cmd.hasOption('h')) {
            printHelpAndExit();
        }
        new RunUpdateWallets2(cmd).runProcess();
    }

    public RunUpdateWallets2(CommandLine cmd) throws SQLException {
        stopFile = new File(cmd.getOptionValue("stop-file", DEFAULT_STOP_FILE_NAME));
        startFromFile = new Utils.NumberFile(cmd.getOptionValue("start-from", Integer.toString(DEFAULT_FIRST_TRANSACTION)));
        firstTransaction = startFromFile.getNumber().intValue();
        batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", Integer.toString(DEFAULT_BATCH_SIZE)));
        txnThreads = Integer.parseInt(cmd.getOptionValue("threads", Integer.toString(DEFAULT_TXN_THREADS)));
        DBConnectionSupplier.applyArguments(cmd);

        conn = new DBConnectionSupplier();
        psUpdateAddressWalletPerTable = Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal)
                .map(type -> conn.prepareStatement(fixAddressTableName(SQL_UPDATE_ADDRESS_WALLET, type))).collect(Collectors.toList());
        queryTransaction = new DbQueryTransaction(conn);
        queryInput = new DbQueryInput(conn);
        queryWallet = new DbQueryWallet(conn);
        dbQueries = new DbQueries(conn);
        execTransactionThreads = Executors.newFixedThreadPool(txnThreads);
    }

    private void runProcess() throws SQLException, InterruptedException {
        log.info("START");
        try {
            try (DbUpdateAddress updateAddress = new DbUpdateAddress(conn);
                    DbUpdateWallet addWallet = new DbUpdateWallet(conn)) {
                initProcess(addWallet);
                int endTransaction = queryTransaction.getLastTransactionId().orElse(0);
                int batchFirstTransaction = firstTransaction;
                for (int loop = 0; batchFirstTransaction <= endTransaction; loop++, batchFirstTransaction += batchSize) {
                    startFromFile.updateNumber(batchFirstTransaction);
                    if (stopFile.exists()) {
                        log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                        stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                        break;
                    }
                    int batchLastTransaction = batchFirstTransaction + batchSize - 1;
                    log.debug("Batch loop #{}. Process transactions [{} - {}]", loop, batchFirstTransaction, batchLastTransaction);
                    processBatch(batchFirstTransaction, batchLastTransaction, updateAddress, addWallet);
                    addWallet.flushCache();
                }
            }
        } finally {
            execAddressQueries.shutdown();
            execTransactionThreads.shutdown();
            log.info("FINISH");
        }
    }

    private void initProcess(DbUpdateWallet addWallet) throws SQLException, InterruptedException {
        log.debug("Checking for missing wallet records...");
        long s = System.currentTimeMillis();
        Collection<Integer> missing = queryWallet.getMissingWalletsParallel();
        log.debug("Missing records: {}. Runtime: {} sec.", missing.size(), (System.currentTimeMillis() - s) / 1000);
        missing.stream().forEach(w -> addWallet.add(w));
        addWallet.flushCache();
        log.debug("Checking for unused wallet records...");
        s = System.currentTimeMillis();
        unusedWallets.addAll(queryWallet.getUnusedWalletRecordsParallel());
        log.debug("Unused wallets: {}. Runtime {} sec.", unusedWallets.size(), (System.currentTimeMillis() - s) / 1000);
        if (!unusedWallets.isEmpty()) {
            log.warn("Found {} unused wallet records: {}", unusedWallets.size(), unusedWallets);
        }
    }

    private void processBatch(int minTxn, int maxTxn, DbUpdateAddress updateAddress, DbUpdateWallet addWallet) throws SQLException, InterruptedException {
        final Map<Integer, Map<Integer, Integer>> needToProcess = getNeedToProccessTxnList(minTxn, maxTxn);
        proccessTxnList(needToProcess, updateAddress, addWallet);
    }

    private Map<Integer, Map<Integer, Integer>> getNeedToProccessTxnList(int minTxn, int maxTxn) throws SQLException, InterruptedException {
        final Map<Integer, Map<Integer, Integer>> result = new HashMap<>();
        execTransactionThreads.invokeAll(queryTransaction.getTxnsRangle(minTxn, maxTxn).stream().filter(tx -> tx.getNInputs() != 0).map(tx -> (Callable<Object>) () -> {
            Collection<BtcAddress> addresses = queryInput.getInputAddresses(tx.getTransactionId());
            //Collection<BtcAddress> addresses = dbQueries.getRelatedAddresses(tx.getTransactionId());
            if (addresses == null || addresses.isEmpty()) {
                log.warn("Unexpected: Addresses list is empty for transactionId " + tx.getTransactionId());
            } else {
                List<Integer> wallets = addresses.stream().map(BtcAddress::getWalletId).distinct().sorted().collect(Collectors.toList());
                if (wallets.isEmpty()) {
                    log.warn("Unexpected: Wallets list is empty for transactionId " + tx.getTransactionId());
                } else if (wallets.get(0) == 0 || wallets.size() > 1) {
                    synchronized (result) {
                        result.put(tx.getTransactionId(), addresses.stream().collect(Collectors.toMap(BtcAddress::getAddressId, BtcAddress::getWalletId)));
                    }
                }
            }
            return null;
        }).collect(Collectors.toList()));
        return result;
    }

    private void proccessTxnList(Map<Integer, Map<Integer, Integer>> needToProcess, DbUpdateAddress updateAddress, DbUpdateWallet addWallet) throws SQLException {
        AtomicInteger newWalletsAssigned = new AtomicInteger();
        AtomicInteger walletsMerged = new AtomicInteger();
        for (Map.Entry<Integer, Map<Integer, Integer>> proc : new ArrayList<>(needToProcess.entrySet())) {
            needToProcess.remove(proc.getKey());
            Map<Integer, Integer> addresses = proc.getValue();
            //List<BtcAddress> addresses = queryInput.getInputAddresses(proc.getKey());
            List<Integer> wallets = addresses.values().stream().distinct().sorted().collect(Collectors.toList());
            if (!wallets.isEmpty() && wallets.get(0) == 0) {
                //wallet is not assigned yet
                int newWalletId = wallets.size() > 1 ? wallets.get(1) : getNextWalletId(addWallet);
                addresses.entrySet().stream().filter(a -> a.getValue() == 0).forEach(a -> updateWallet(a.getKey(), newWalletId, needToProcess, updateAddress));
                //updateAddress.flushCache();
                wallets.remove(0);
                newWalletsAssigned.incrementAndGet();
            }
            if (wallets.size() > 1) {
                //merge multiple wallets
                int walletToUse = wallets.get(0);
                wallets.stream().filter(w -> w != walletToUse).forEach(w -> replaceWallet(walletToUse, w, needToProcess));
                walletsMerged.addAndGet(wallets.size() - 1);
            }
        }
        updateAddress.flushCache();
        if (newWalletsAssigned.get() != 0 || walletsMerged.get() != 0) {
            log.info("newWalletsAssigned: {}, walletsMerged={}", newWalletsAssigned, walletsMerged);
        }
    }

    private void updateWallet(int addressId, int newWalletId, Map<Integer, Map<Integer, Integer>> needToProcess, DbUpdateAddress updateAddress) {
        updateAddress.updateWallet(addressId, newWalletId);
        updateCacheByAddress(addressId, newWalletId, needToProcess);
    }

    private int replaceWallet(int walletToUse, int walletToReplace, Map<Integer, Map<Integer, Integer>> needToProcess) {
        if (walletToUse == 0 || walletToReplace == 0 || walletToUse == walletToReplace) {
            throw new IllegalArgumentException("walletToUse=" + walletToUse + ", walletToReplace=" + walletToReplace);
        }
        try {
            final AtomicInteger nUpdated = new AtomicInteger();
            execAddressQueries.invokeAll(psUpdateAddressWalletPerTable.stream().map(ps -> (Callable<Object>) () -> {
                ps.getParamSetter().setInt(walletToUse).setInt(walletToReplace).checkStateReady();
                nUpdated.addAndGet(ps.executeUpdate());
                return null;
            }).collect(Collectors.toList()));
            log.debug("Merging wallets: ({},{})=>{}. Records updated: {}", walletToUse, walletToReplace, walletToUse, nUpdated);
            unusedWallets.add(walletToReplace);
            updateCacheByWallet(walletToUse, walletToReplace, needToProcess);
            return nUpdated.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update all cached addressId with new walletId.
     *
     * @param addressId
     * @param newWalletId
     * @param needToProcess
     */
    private void updateCacheByAddress(int addressId, int newWalletId, Map<Integer, Map<Integer, Integer>> needToProcess) {
        needToProcess.forEach((t, m) -> m.replace(addressId, newWalletId));
    }

    /**
     * Update all cached addresses with new walletId.
     *
     * @param walletToUse
     * @param walletToReplace
     * @param needToProcess
     */
    private void updateCacheByWallet(int walletToUse, int walletToReplace, Map<Integer, Map<Integer, Integer>> needToProcess) {
        needToProcess.forEach((t, m) -> m.entrySet().stream().filter(e -> e.getValue() == walletToReplace).forEach(e -> e.setValue(walletToUse)));
    }

    private int getNextWalletId(DbUpdateWallet addWallet) throws SQLException {
        synchronized (unusedWallets) {
            if (!unusedWallets.isEmpty()) {
                Integer w = unusedWallets.iterator().next();
                unusedWallets.remove(w);
                return w;
            }
        }
        return addWallet.add().getWalletId();
    }

    private String fixAddressTableName(String sql, SrcAddressType addressType) {
        return sql.replaceAll("address_table_name", "address_" + addressType.name().toLowerCase());
    }

    private static void printHelpAndExit() {
        System.out.println("Available options:");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java <jar> " + Main.Command.update_wallets + " [options]", prepOptions());
        System.exit(1);
    }

    private static Options prepOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        options.addOption(null, "start-from", true, "First transaction Id to process. Beside a number this parameter can be set to a file name thats stores the numeric value updated on every batch");
        options.addOption(null, "batch-size", true, "Number or transactions to read in a batch. Default: " + DEFAULT_BATCH_SIZE);
        options.addOption(null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end. Default: " + DEFAULT_STOP_FILE_NAME);
        options.addOption("t", "threads", true, "Number of threads to run. Default is " + DEFAULT_TXN_THREADS);
        DBConnectionSupplier.addOptions(options);
        return options;
    }
}
