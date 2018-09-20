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

import com.sliva.btc.scanner.Main;
import com.sliva.btc.scanner.db.DBConnection;
import com.sliva.btc.scanner.db.DBUtils;
import com.sliva.btc.scanner.db.DbAddWallet;
import com.sliva.btc.scanner.db.DbUpdateAddress;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcWallet;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.IntCollection;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunUpdateWallets {

    private static final int DEFAULT_TXN_THREADS = 20;
    private static final int DEFAULT_READ_ROWS_BATCH = 10000;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-update-wallet-stop";
    private static final SrcAddressType DEFAULT_ADDRESS_TYPE = SrcAddressType.P2PKH;
    private static final String SQL_QUERY_ADDRESSES_NO_WALLET
            = "SELECT address_id FROM address_table_name"
            + " WHERE wallet_id=0"
            + " AND address_id IN (SELECT address_id FROM output O INNER JOIN input I ON I.in_transaction_id=O.transaction_id AND I.in_pos=O.pos)";
    private static final String SQL_QUERY_RELATED_ADDRESSES
            //            = "SELECT DISTINCT O2.address_id, IFNULL(p2pkh.wallet_id, IFNULL(p2sh.wallet_id, IFNULL(p2wpkh.wallet_id, p2wsh.wallet_id)))"
            //            + " FROM output O"
            //            + " INNER JOIN input I ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            //            + " INNER JOIN input I2 ON I2.transaction_id = I.transaction_id"
            //            + " INNER JOIN output O2 ON O2.transaction_id=I2.in_transaction_id AND O2.pos=I2.in_pos"
            //            + " LEFT JOIN address_p2pkh P2PKH ON P2PKH.address_id = O2.address_id"
            //            + " LEFT JOIN address_p2sh P2SH ON P2SH.address_id = O2.address_id"
            //            + " LEFT JOIN address_p2wpkh P2WPKH ON P2WPKH.address_id = O2.address_id"
            //            + " LEFT JOIN address_p2wsh P2WSH ON P2WSH.address_id = O2.address_id"
            //            + " WHERE O.address_id = ?";
            = "SELECT DISTINCT O2.address_id,IFNULL(P2PKH.wallet_id, IFNULL(P2SH.wallet_id, IFNULL(P2WPKH.wallet_id, P2WSH.wallet_id)))"
            + " FROM input I2"
            + " INNER JOIN output O2 ON O2.transaction_id=I2.in_transaction_id AND O2.pos=I2.in_pos"
            + " LEFT JOIN address_p2pkh P2PKH ON P2PKH.address_id=O2.address_id"
            + " LEFT JOIN address_p2sh P2SH ON P2SH.address_id=O2.address_id"
            + " LEFT JOIN address_p2wpkh P2WPKH ON P2WPKH.address_id=O2.address_id"
            + " LEFT JOIN address_p2wsh P2WSH ON P2WSH.address_id=O2.address_id"
            + " WHERE I2.transaction_id IN"
            + "(SELECT DISTINCT I.transaction_id"
            + " FROM output O"
            + " INNER JOIN input I ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE O.address_id = ?)";
    private static final String SQL_QUERY_RELATED_WALLETS
            //            = "SELECT DISTINCT IFNULL(p2pkh.wallet_id, IFNULL(p2sh.wallet_id, IFNULL(p2wpkh.wallet_id, p2wsh.wallet_id)))"
            //            + " FROM (SELECT * FROM address_p2pkh WHERE wallet_id = ?"
            //            + " UNION SELECT * FROM address_p2sh WHERE wallet_id = ?"
            //            + " UNION SELECT * FROM address_p2wpkh WHERE wallet_id = ?"
            //            + " UNION SELECT * FROM address_p2wsh WHERE wallet_id = ?) A"
            //            + " INNER JOIN output O ON O.address_id=A.address_id"
            //            + " INNER JOIN input I ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            //            + " INNER JOIN input I2 ON I2.transaction_id = I.transaction_id"
            //            + " INNER JOIN output O2 ON O2.transaction_id=I2.in_transaction_id AND O2.pos=I2.in_pos"
            //            + " LEFT JOIN address_p2pkh p2pkh ON p2pkh.address_id=O2.address_id"
            //            + " LEFT JOIN address_p2sh p2sh ON p2sh.address_id=O2.address_id"
            //            + " LEFT JOIN address_p2wpkh p2wpkh ON p2wpkh.address_id=O2.address_id"
            //            + " LEFT JOIN address_p2wsh p2wsh ON p2wsh.address_id=O2.address_id";
            = "SELECT DISTINCT IFNULL(P2PKH.wallet_id, IFNULL(P2SH.wallet_id, IFNULL(P2WPKH.wallet_id, P2WSH.wallet_id)))"
            + " FROM input I2"
            + " INNER JOIN output O2 ON O2.transaction_id=I2.in_transaction_id AND O2.pos=I2.in_pos"
            + " LEFT JOIN address_p2pkh P2PKH ON P2PKH.address_id=O2.address_id"
            + " LEFT JOIN address_p2sh P2SH ON P2SH.address_id=O2.address_id"
            + " LEFT JOIN address_p2wpkh P2WPKH ON P2WPKH.address_id=O2.address_id"
            + " LEFT JOIN address_p2wsh P2WSH ON P2WSH.address_id=O2.address_id "
            + " WHERE I2.transaction_id IN (SELECT DISTINCT I.transaction_id"
            + " FROM (SELECT address_id FROM address_p2pkh WHERE wallet_id = ?"
            + " UNION SELECT address_id FROM address_p2sh WHERE wallet_id = ?"
            + " UNION SELECT address_id FROM address_p2wpkh WHERE wallet_id = ?"
            + " UNION SELECT address_id FROM address_p2wsh WHERE wallet_id = ?) A"
            + " INNER JOIN output O ON O.address_id=A.address_id"
            + " INNER JOIN input I ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos)";
    private static final String SQL_QUERY_RELATED_WALLETS_BY_ADDRESS
            = "SELECT DISTINCT IFNULL(P2PKH.wallet_id, IFNULL(P2SH.wallet_id, IFNULL(P2WPKH.wallet_id, P2WSH.wallet_id)))"
            + " FROM input I2"
            + " INNER JOIN output O2 ON O2.transaction_id=I2.in_transaction_id AND O2.pos=I2.in_pos"
            + " LEFT JOIN address_p2pkh P2PKH ON P2PKH.address_id=O2.address_id"
            + " LEFT JOIN address_p2sh P2SH ON P2SH.address_id=O2.address_id"
            + " LEFT JOIN address_p2wpkh P2WPKH ON P2WPKH.address_id=O2.address_id"
            + " LEFT JOIN address_p2wsh P2WSH ON P2WSH.address_id=O2.address_id "
            + " WHERE I2.transaction_id IN (SELECT DISTINCT I.transaction_id"
            + " FROM output O"
            + " INNER JOIN input I ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE O.address_id=?)";
    private static final String SQL_UPDATE_ADDRESS_WALLET
            = "UPDATE address_table_name SET wallet_id=? WHERE wallet_id=?";
    private static final String SQL_QUERY_MERGE_TRANSACTION
            = "SELECT DISTINCT A1.address_id,A1.address,A2.address_id,A2.address,T.txid FROM input I1"
            + " INNER JOIN input I2 ON I1.transaction_id=I2.transaction_id"
            + " INNER JOIN output O1 ON O1.transaction_id=I1.in_transaction_id AND O1.pos=I1.in_pos"
            + " INNER JOIN output O2 ON O2.transaction_id=I2.in_transaction_id AND O2.pos=I2.in_pos"
            + " INNER JOIN address_table_name A1 ON A1.address_id=O1.address_id"
            + " INNER JOIN address_table_name A2 ON A2.address_id=O2.address_id"
            + " INNER JOIN transaction T ON T.transaction_id=I1.transaction_id"
            + " WHERE A1.wallet_id=?"
            + " AND A2.wallet_id=?";
    private static final String SQL_QUERY_ADDRESS_IDS_BY_WALLET
            = "SELECT address_id FROM address_p2pkh WHERE wallet_id = ?"
            + " UNION SELECT address_id FROM address_p2sh WHERE wallet_id = ?"
            + " UNION SELECT address_id FROM address_p2wpkh WHERE wallet_id = ?"
            + " UNION SELECT address_id FROM address_p2wsh WHERE wallet_id = ?";
    private static final String SQL_QUERY_ADDRESSES_BY_WALLET
            = "SELECT address_id,address FROM address_p2pkh WHERE wallet_id = ?"
            + " UNION SELECT address_id,address FROM address_p2sh WHERE wallet_id = ?"
            + " UNION SELECT address_id,address FROM address_p2wpkh WHERE wallet_id = ?"
            + " UNION SELECT address_id,address FROM address_p2wsh WHERE wallet_id = ?";
    private static final String SQL_QUERY_UNUSED_WALLETS = "SELECT wallet_id FROM wallet"
            + " WHERE wallet_id NOT IN (SELECT wallet_id FROM address_p2pkh WHERE wallet_id>0)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2sh WHERE wallet_id>0)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2wpkh WHERE wallet_id>0)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2wsh WHERE wallet_id>0)";
    private static final String SQL_QUERY_ALL_ADDRESSES_BY_WALLET
            = "SELECT address_id,address,'P2PKH' FROM address_p2pkh WHERE wallet_id=?"
            + " UNION SELECT address_id,address,'P2SH' FROM address_p2sh WHERE wallet_id=?"
            + " UNION SELECT address_id,address,'P2WPKH' FROM address_p2wpkh WHERE wallet_id=?"
            + " UNION SELECT address_id,address,'P2WSH' FROM address_p2wsh WHERE wallet_id=?";
    private static final String SQL_QUERY_MISSING_WALLET_RECORDS
            = "SELECT wallet_id FROM address_p2pkh WHERE wallet_id>0 AND wallet_id not in (select wallet_id from wallet)"
            + " UNION SELECT wallet_id FROM address_p2sh WHERE wallet_id>0 AND wallet_id not in (select wallet_id from wallet)"
            + " UNION SELECT wallet_id FROM address_p2wpkh WHERE wallet_id>0 AND wallet_id not in (select wallet_id from wallet)"
            + " UNION SELECT wallet_id FROM address_p2wsh WHERE wallet_id>0 AND wallet_id not in (select wallet_id from wallet)";
    private static final String SQL_QUERY_TRANSACTIONS_WITH_DIFFERENT_INPUT_WALLETS
            = "SELECT transaction_id,SUM(IF(wallet_id>0,1,0)) as num FROM ("
            + "SELECT DISTINCT I.transaction_id,A.wallet_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " INNER JOIN address_p2pkh A ON A.address_id=O.address_id"
            + " WHERE I.transaction_id BETWEEN ? AND ?"
            + " ORDER BY I.transaction_id,A.wallet_id"
            + " ) S"
            + " WHERE wallet_id>0"
            + " GROUP BY transaction_id"
            + " HAVING num>1";
    private static final String SQL_QUERY_WALLETS_BY_TRANSACTION
            = "SELECT DISTINCT A.wallet_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " INNER JOIN address_p2pkh A ON A.address_id=O.address_id"
            + " WHERE A.wallet_id>0 AND I.transaction_id=?";
    private static final String SQL_QUERY_SPEND_TRANSACTIONS_BY_ADDRESS
            = "SELECT I.transaction_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE O.address_id=?";
    private final DBConnection dbCon;
    private final ThreadLocal<PreparedStatement> psAddressesNoWallet;
    private final ThreadLocal<PreparedStatement> psRelatedAddresses;
    private final ThreadLocal<PreparedStatement> psRelatedWallets;
    private final ThreadLocal<PreparedStatement> psRelatedWalletsByAddress;
//    private  final ThreadLocal<PreparedStatement> psUpdateAddressWallet;
    private final ThreadLocal<PreparedStatement> psQueryMergeTransaction;
    private final ThreadLocal<PreparedStatement> psQueryAddressIdsByWallet;
    private final ThreadLocal<PreparedStatement> psQueryAddressesByWallet;
    private final ThreadLocal<PreparedStatement> psQueryUnusedWallets;
    private final ThreadLocal<PreparedStatement> psQueryAllAddressesByWallet;
    private final ThreadLocal<PreparedStatement> psQueryMissingWalletRecords;
    private final ThreadLocal<PreparedStatement> psQueryTransactionsWithDifferentInputWallets;
    private final ThreadLocal<PreparedStatement> psQueryWalletdByTransaction;
    private final ThreadLocal<PreparedStatement> psQuerySpendTransactionsByAddress;
    private final Collection<ThreadLocal<PreparedStatement>> psUpdateAddressWalletPerTable;
    private final ExecutorService execQuery;
    private final File stopFile;
    private final Set<Integer> addressesToUpdate = new HashSet<>();
    private final Set<Integer> unusedWallets = new HashSet<>();
    private final SrcAddressType addressType;
    private final int readRowsBatch;

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
        new RunUpdateWallets(cmd).runProcess();
    }

    public RunUpdateWallets(CommandLine cmd) {
        stopFile = new File(cmd.getOptionValue("stop-file", DEFAULT_STOP_FILE_NAME));
        readRowsBatch = Integer.parseInt(cmd.getOptionValue("batch-size", Integer.toString(DEFAULT_READ_ROWS_BATCH)));
        addressType = SrcAddressType.valueOf(cmd.getOptionValue("address-type", DEFAULT_ADDRESS_TYPE.name()));
        execQuery = Executors.newFixedThreadPool(Integer.parseInt(cmd.getOptionValue("threads", Integer.toString(DEFAULT_TXN_THREADS))));
        DBConnection.applyArguments(cmd);

        dbCon = new DBConnection();
        psAddressesNoWallet = dbCon.prepareStatement(fixTableName(SQL_QUERY_ADDRESSES_NO_WALLET));
        psRelatedAddresses = dbCon.prepareStatement(fixTableName(SQL_QUERY_RELATED_ADDRESSES));
        psRelatedWallets = dbCon.prepareStatement(fixTableName(SQL_QUERY_RELATED_WALLETS));
        psRelatedWalletsByAddress = dbCon.prepareStatement(fixTableName(SQL_QUERY_RELATED_WALLETS_BY_ADDRESS));
//        psUpdateAddressWallet = dbCon.prepareStatement(fixTableName(SQL_UPDATE_ADDRESS_WALLET));
        psQueryMergeTransaction = dbCon.prepareStatement(fixTableName(SQL_QUERY_MERGE_TRANSACTION));
        psQueryAddressIdsByWallet = dbCon.prepareStatement(fixTableName(SQL_QUERY_ADDRESS_IDS_BY_WALLET));
        psQueryAddressesByWallet = dbCon.prepareStatement(fixTableName(SQL_QUERY_ADDRESSES_BY_WALLET));
        psQueryUnusedWallets = dbCon.prepareStatement(fixTableName(SQL_QUERY_UNUSED_WALLETS));
        psQueryAllAddressesByWallet = dbCon.prepareStatement(fixTableName(SQL_QUERY_ALL_ADDRESSES_BY_WALLET));
        psQueryMissingWalletRecords = dbCon.prepareStatement(fixTableName(SQL_QUERY_MISSING_WALLET_RECORDS));
        psQueryTransactionsWithDifferentInputWallets = dbCon.prepareStatement(fixTableName(SQL_QUERY_TRANSACTIONS_WITH_DIFFERENT_INPUT_WALLETS));
        psQueryWalletdByTransaction = dbCon.prepareStatement(fixTableName(SQL_QUERY_WALLETS_BY_TRANSACTION));
        psQuerySpendTransactionsByAddress = dbCon.prepareStatement(fixTableName(SQL_QUERY_SPEND_TRANSACTIONS_BY_ADDRESS));

        psUpdateAddressWalletPerTable = new ArrayList<>(BtcAddress.getRealTypes().size());
        BtcAddress.getRealTypes().forEach((at) -> psUpdateAddressWalletPerTable.add(dbCon.prepareStatement(fixTableName(SQL_UPDATE_ADDRESS_WALLET, at))));
    }

    private void runProcess() throws SQLException, InterruptedException {
        log.info("START");

        fixWalletsByTransactions(0_000_000, 8_000_000);

        try (DbAddWallet addWallet = new DbAddWallet(dbCon);
                DbUpdateAddress updateAddress = new DbUpdateAddress(dbCon)) {

            try (ResultSet rs = psQueryMissingWalletRecords.get().executeQuery()) {
                while (rs.next()) {
                    int walletId = rs.getInt(1);
                    log.warn("Found missing wallet record: {}", walletId);
                    addWallet.add(BtcWallet.builder().walletId(walletId).build());
                }
                addWallet.flushCache();
            }

            try (ResultSet rs = psQueryUnusedWallets.get().executeQuery()) {
                while (rs.next()) {
                    unusedWallets.add(rs.getInt(1));
                }
            }
            log.info("Unused wallets loaded: {}", unusedWallets.size());
//            if (true) return;

            for (int batch = 0;; batch++) {
                if (stopFile.exists()) {
                    log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                    stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                    break;
                }
                log.debug("Start Batch #{}", batch);
                psAddressesNoWallet.get().setMaxRows(readRowsBatch);
                try (ResultSet rs = psAddressesNoWallet.get().executeQuery()) {
                    while (rs.next()) {
                        addressesToUpdate.add(rs.getInt(1));
                    }
                }
                int loops = 0;
                while (!addressesToUpdate.isEmpty()) {
                    int addressId;
                    synchronized (addressesToUpdate) {
                        Iterator<Integer> it = addressesToUpdate.iterator();
                        addressId = it.next();
                        it.remove();
                    }
                    processAddress(addressId, addWallet, updateAddress);
                    updateAddress.flushCache();
                    loops++;
                }
                log.debug("Loops: {}", loops);
                addWallet.flushCache();
            }
        } finally {
            execQuery.shutdown();
            log.info("FINISH");
        }
    }

    private void processAddress(int addressId, DbAddWallet addWallet, DbUpdateAddress updateAddress) throws SQLException, InterruptedException {
        psRelatedAddresses.get().setInt(1, addressId);
        psRelatedAddresses.get().setFetchSize(Integer.MIN_VALUE);
        Set<Integer> relatedWallets = new HashSet<>();
        IntCollection zeroWalletAddresses = new IntCollection(10000);
        try (ResultSet rs = psRelatedAddresses.get().executeQuery()) {
            int n = 0;
            while (rs.next()) {
                int adrId = rs.getInt(1);
                Integer walletId = rs.getObject(2, Integer.class);
                if (walletId == null) {
                    log.warn("Unexpected: wallet_id is null for address_id={}", adrId);
                } else {
                    if (walletId != 0) {
                        relatedWallets.add(walletId);
                    } else {
                        zeroWalletAddresses.add(adrId);
                    }
                }
                if (++n % 1000000 == 0) {
                    log.debug("Loading addresses from DB: {} M", n / 1000000);
                }
            }
        }
        Collection<Integer> transactionsToCheck = new HashSet<>();
        if (relatedWallets.isEmpty() && zeroWalletAddresses.isEmpty()) {
            //address never been spent - assign new wallet_id
            BtcAddress adr = BtcAddress.builder().addressId(addressId).walletId(getNextWalletId(addWallet)).build();
            log.trace("updateAddress[1]: {}", adr);
            updateAddress.updateWallet(adr);
            transactionsToCheck.addAll(getSpendTransactionsByAddress(adr.getAddressId()));
            updateAddress.flushCache();
        } else {
            //List<Integer> relatedWallets = awMap.values().stream().filter((i) -> i != 0).distinct().collect(Collectors.toList());
            int wCount = relatedWallets.size();
            final int walletId;
            if (wCount == 0) {
                //no wallet assigned yet to any related addresses
                walletId = getNextWalletId(addWallet);
            } else {
                //take smallest wallet ID as main
                walletId = relatedWallets.stream().min((i, j) -> Integer.compare(i, j)).get();
            }
            int addrCount = zeroWalletAddresses.getSize();
            if (addrCount > 1000000) {
                log.debug("Addresses loaded: {}", addrCount);
            }
            for (int i = 0; i < addrCount; i++) {
                BtcAddress adr = BtcAddress.builder().addressId(zeroWalletAddresses.get(i)).walletId(walletId).build();
                log.trace("updateAddress[2]: {}", adr);
                updateAddress.updateWallet(adr);
                addressesToUpdate.remove(zeroWalletAddresses.get(i));
                transactionsToCheck.addAll(getSpendTransactionsByAddress(adr.getAddressId()));
            }
            updateAddress.flushCache();
            if (wCount > 1) {
                log.debug("multiple wallets found in related addresses: " + relatedWallets);
                processWallet(walletId, relatedWallets);
            }
        }
        log.trace("Transactions to check and fix: {}", transactionsToCheck.size());
        for (Integer transactionId : transactionsToCheck) {
            fixTransaction(transactionId);
        }
    }

    private Collection<Integer> getSpendTransactionsByAddress(int addressId) throws SQLException {
        psQuerySpendTransactionsByAddress.get().setInt(1, addressId);
        return DBUtils.readIntegers(psQuerySpendTransactionsByAddress.get());
    }

    private int processWallet(int walletId, Collection<Integer> relatedWallets) throws SQLException, InterruptedException {
        if (walletId == 0) {
            log.error("processWallet: walletId=0");
            return 0;
        }
        final AtomicInteger result = new AtomicInteger(0);
        relatedWallets.stream().filter((i) -> i > walletId).forEach((a) -> {
            log.debug("Merging wallets: ({}, {}) => {}", walletId, a, walletId);
//            printMergeDetails(walletId, a);
            try {
                int nUpdated = 0;
                for (ThreadLocal<PreparedStatement> ps : psUpdateAddressWalletPerTable) {
                    ps.get().setInt(1, walletId);
                    ps.get().setInt(2, a);
                    nUpdated += ps.get().executeUpdate();
                }
                result.addAndGet(nUpdated);
                log.debug("Records merged: {}", nUpdated);
                if (isWalletUsed(a)) {
                    log.warn("Unexpected: Wallet is still used: {}", a);
                } else {
                    synchronized (unusedWallets) {
                        unusedWallets.add(a);
                    }
                }
            } catch (SQLException ex) {
                log.error(null, ex);
            }
        });
        //re-process merged wallet
//        reProcessWallet(walletId);
        return result.get();
    }

    private void reProcessWallet(int walletId) throws SQLException, InterruptedException {
        Collection<Integer> addr = queryAddressIdsByWalletId(walletId);
        if (addr.isEmpty()) {
            return;
        }
        final Set<Integer> relatedWallets = new HashSet<>();
        if (addr.size() < 100) {
            psRelatedWallets.get().setInt(1, walletId);
            psRelatedWallets.get().setInt(2, walletId);
            psRelatedWallets.get().setInt(3, walletId);
            psRelatedWallets.get().setInt(4, walletId);
            try (ResultSet rs = psRelatedWallets.get().executeQuery()) {
                while (rs.next()) {
                    Integer w = rs.getObject(1, Integer.class);
                    if (w == null) {
                        log.warn("Unexpected: wallet_id is null. Original wallet ID: {}", walletId);
                    } else {
                        if (w != 0) {
                            relatedWallets.add(w);
                        }
                    }
                }
            }
        } else {
            //if wallet has too many addresses - run query for one address at a time 
            List<Callable<Boolean>> todo = new ArrayList<>(addr.size());
            for (Integer addressId : addr) {
                todo.add(() -> {
                    psRelatedWalletsByAddress.get().setInt(1, addressId);
                    try (ResultSet rs = psRelatedWalletsByAddress.get().executeQuery()) {
                        while (rs.next()) {
                            Integer w = rs.getObject(1, Integer.class);
                            if (w == null) {
                                log.warn("Unexpected: wallet_id is null. Original wallet ID: {}", walletId);
                            } else {
                                if (w != 0) {
                                    synchronized (relatedWallets) {
                                        relatedWallets.add(w);
                                    }
                                }
                            }
                        }
                    }
                    return true;
                });
            }
            execQuery.invokeAll(todo);
        }
        if (relatedWallets.size() > 1) {
            log.debug("multiple related wallets found on re-processing. Original wallet ID: {}, related wallets: {}", walletId, relatedWallets);
            int minWalletId = relatedWallets.stream().min((i, j) -> Integer.compare(i, j)).get();
            processWallet(minWalletId, relatedWallets);
        }
    }

    private Collection<Integer> queryAddressIdsByWalletId(int walletId) throws SQLException {
        psQueryAddressIdsByWallet.get().setInt(1, walletId);
        psQueryAddressIdsByWallet.get().setInt(2, walletId);
        psQueryAddressIdsByWallet.get().setInt(3, walletId);
        psQueryAddressIdsByWallet.get().setInt(4, walletId);
        return DBUtils.readIntegers(psQueryAddressIdsByWallet.get());
    }

    private int fixWalletsByTransactions(int minTransactionId, int maxTransactionId) throws SQLException, InterruptedException {
        log.debug("fixWalletsByTransactions(minTransactionId:{}, maxTransactionId:{})", minTransactionId, maxTransactionId);
        psQueryTransactionsWithDifferentInputWallets.get().setInt(1, minTransactionId);
        psQueryTransactionsWithDifferentInputWallets.get().setInt(2, maxTransactionId);
        final AtomicInteger result = new AtomicInteger(0);
        for (Integer transactionId : DBUtils.readIntegers(psQueryTransactionsWithDifferentInputWallets.get())) {
            log.debug("Fixing transactionId: {}", transactionId);
            int nUpdated = fixTransaction(transactionId);
            result.addAndGet(nUpdated);
        }
        return result.get();
    }

    private int fixTransaction(int transactionId) throws SQLException, InterruptedException {
        psQueryWalletdByTransaction.get().setInt(1, transactionId);
        Collection<Integer> wList = DBUtils.readIntegers(psQueryWalletdByTransaction.get());
        if (wList.size() < 2) {
            log.trace("Transaction {} has been fixed already: {}", transactionId, wList);
            return 0;
        }
        int minWalletId = wList.stream().min((i, j) -> Integer.compare(i, j)).get();
        log.debug("fixTransaction(transactionId:{}): minWalletId={}, wList={}", transactionId, minWalletId, wList);
        return processWallet(minWalletId, wList);
    }

    private int getNextWalletId(DbAddWallet addWallet) throws SQLException {
        synchronized (unusedWallets) {
            if (!unusedWallets.isEmpty()) {
                Integer w = unusedWallets.iterator().next();
                unusedWallets.remove(w);
                return w;
            }
        }
        return addWallet.add(null).getWalletId();
    }

    private boolean isWalletUsed(int walletId) throws SQLException {
        psQueryAllAddressesByWallet.get().setInt(1, walletId);
        psQueryAllAddressesByWallet.get().setInt(2, walletId);
        psQueryAllAddressesByWallet.get().setInt(3, walletId);
        psQueryAllAddressesByWallet.get().setInt(4, walletId);
        psQueryAllAddressesByWallet.get().setMaxRows(1);
        try (ResultSet rs = psQueryAllAddressesByWallet.get().executeQuery()) {
            return rs.next();
        }
    }

    private void printMergeDetails(int walletId1, int walletId2) {
        if (log.isDebugEnabled()) {
            try {
                printWalletAddresses(walletId1);
                printWalletAddresses(walletId2);
                psQueryMergeTransaction.get().setInt(1, walletId1);
                psQueryMergeTransaction.get().setInt(2, walletId2);
                try (ResultSet rs = psQueryMergeTransaction.get().executeQuery()) {
                    while (rs.next()) {
                        BtcAddress adr1 = BtcAddress.builder().addressId(rs.getInt(1)).address(rs.getBytes(2)).walletId(walletId1).build();
                        BtcAddress adr2 = BtcAddress.builder().addressId(rs.getInt(3)).address(rs.getBytes(4)).walletId(walletId2).build();
                        String txid = Hex.encodeHexString(rs.getBytes(5));
                        log.debug("[{},{}]: Addresses {} and {} merged in transaction {}", walletId1, walletId2, adr1.getBjAddress(), adr2.getBjAddress(), txid);
                    }
                }
            } catch (SQLException e) {
                log.error("walletId1=" + walletId1 + ", walletId2=" + walletId2, e);
            }
        }
    }

    private void printWalletAddresses(int walletId) throws SQLException {
        if (log.isTraceEnabled()) {
            log.trace("Wallet#{} addresses: ", walletId);
            psQueryAddressesByWallet.get().setInt(1, walletId);
            psQueryAddressesByWallet.get().setInt(2, walletId);
            psQueryAddressesByWallet.get().setInt(3, walletId);
            psQueryAddressesByWallet.get().setInt(4, walletId);
            try (ResultSet rs = psQueryAddressesByWallet.get().executeQuery()) {
                while (rs.next()) {
                    BtcAddress adr = BtcAddress.builder().addressId(rs.getInt(1)).address(rs.getBytes(2)).walletId(walletId).build();
                    log.trace("{}, id={}", adr.getBjAddress(), adr.getAddressId());
                }
            }
        }
    }

    private String getTableName(SrcAddressType addressType) {
        return "address_" + addressType.name().toLowerCase();
    }

    private String getTableName() {
        return getTableName(addressType);
    }

    private String fixTableName(String sql, SrcAddressType addressType) {
        return sql.replaceAll("address_table_name", getTableName(addressType));
    }

    private String fixTableName(String sql) {
        return fixTableName(sql, addressType);
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
        options.addOption(null, "address-type", true, "Addresses type to scan and update. Acceptable values: " + BtcAddress.getRealTypes() + ". Default: " + DEFAULT_ADDRESS_TYPE);
        options.addOption(null, "batch-size", true, "Number or addresses to read in a batch. Default: " + DEFAULT_READ_ROWS_BATCH);
        options.addOption(null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end. Default: " + DEFAULT_STOP_FILE_NAME);
        options.addOption("t", "threads", true, "Number of threads to run. Default is " + DEFAULT_TXN_THREADS + ". To disable parallel threading set value to 0");
        DBConnection.addOptions(options);
        return options;
    }

}
