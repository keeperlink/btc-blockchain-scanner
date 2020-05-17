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
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.utils.DBUtils;
import static com.sliva.btc.scanner.db.facade.DbQueryAddressOne.getAddressTableName;
import static com.sliva.btc.scanner.db.facade.DbQueryAddressOne.updateQueryTableName;
import com.sliva.btc.scanner.db.facade.DbUpdateAddress;
import com.sliva.btc.scanner.db.facade.DbUpdateWallet;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcWallet;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.IntCollection;
import java.io.File;
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
    private final DBConnectionSupplier dbCon;
    private final DBPreparedStatement psAddressesNoWallet;
    private final DBPreparedStatement psRelatedAddresses;
    private final DBPreparedStatement psRelatedWallets;
    private final DBPreparedStatement psRelatedWalletsByAddress;
//    private  final DBPreparedStatement psUpdateAddressWallet;
    private final DBPreparedStatement psQueryMergeTransaction;
    private final DBPreparedStatement psQueryAddressIdsByWallet;
    private final DBPreparedStatement psQueryAddressesByWallet;
    private final DBPreparedStatement psQueryUnusedWallets;
    private final DBPreparedStatement psQueryAllAddressesByWallet;
    private final DBPreparedStatement psQueryMissingWalletRecords;
    private final DBPreparedStatement psQueryTransactionsWithDifferentInputWallets;
    private final DBPreparedStatement psQueryWalletdByTransaction;
    private final DBPreparedStatement psQuerySpendTransactionsByAddress;
    private final Collection<DBPreparedStatement> psUpdateAddressWalletPerTable;
    private final ExecutorService execQuery;
    private final File stopFile;
    private final Set<Integer> addressesToUpdate = new HashSet<>();
    private final Set<Integer> unusedWallets = new HashSet<>();
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
        SrcAddressType addressType = SrcAddressType.valueOf(cmd.getOptionValue("address-type", DEFAULT_ADDRESS_TYPE.name()));
        execQuery = Executors.newFixedThreadPool(Integer.parseInt(cmd.getOptionValue("threads", Integer.toString(DEFAULT_TXN_THREADS))));
        DBConnectionSupplier.applyArguments(cmd);

        dbCon = new DBConnectionSupplier();
        psAddressesNoWallet = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_ADDRESSES_NO_WALLET, addressType));
        psRelatedAddresses = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_RELATED_ADDRESSES, addressType));
        psRelatedWallets = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_RELATED_WALLETS, addressType));
        psRelatedWalletsByAddress = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_RELATED_WALLETS_BY_ADDRESS, addressType));
//        psUpdateAddressWallet = dbCon.prepareStatement(updateQueryTableName(SQL_UPDATE_ADDRESS_WALLET, addressType));
        psQueryMergeTransaction = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_MERGE_TRANSACTION, addressType));
        psQueryAddressIdsByWallet = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_ADDRESS_IDS_BY_WALLET, addressType));
        psQueryAddressesByWallet = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_ADDRESSES_BY_WALLET, addressType));
        psQueryUnusedWallets = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_UNUSED_WALLETS, addressType));
        psQueryAllAddressesByWallet = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_ALL_ADDRESSES_BY_WALLET, addressType));
        psQueryMissingWalletRecords = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_MISSING_WALLET_RECORDS, addressType));
        psQueryTransactionsWithDifferentInputWallets = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_TRANSACTIONS_WITH_DIFFERENT_INPUT_WALLETS, addressType));
        psQueryWalletdByTransaction = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_WALLETS_BY_TRANSACTION, addressType));
        psQuerySpendTransactionsByAddress = dbCon.prepareStatement(updateQueryTableName(SQL_QUERY_SPEND_TRANSACTIONS_BY_ADDRESS, addressType));

        psUpdateAddressWalletPerTable = new ArrayList<>();
        Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).forEach(at -> psUpdateAddressWalletPerTable.add(dbCon.prepareStatement(updateQueryTableName(SQL_UPDATE_ADDRESS_WALLET, at), getAddressTableName(at) + ".wallet_id")));
    }

    private void runProcess() throws SQLException, InterruptedException {
        log.info("START");

        fixWalletsByTransactions(0_000_000, 8_000_000);

        try (DbUpdateWallet addWallet = new DbUpdateWallet(dbCon);
                DbUpdateAddress updateAddress = new DbUpdateAddress(dbCon)) {

            psQueryMissingWalletRecords.executeQuery(rs
                    -> {
                int walletId = rs.getInt(1);
                log.warn("Found missing wallet record: {}", walletId);
                addWallet.add(BtcWallet.builder().walletId(walletId).build());
            });
            addWallet.flushCache();
            unusedWallets.addAll(DBUtils.readIntegersToSet(psQueryUnusedWallets));
            log.info("Unused wallets loaded: {}", unusedWallets.size());

            for (int batch = 0;; batch++) {
                if (stopFile.exists()) {
                    log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                    stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                    break;
                }
                log.debug("Start Batch #{}", batch);
                addressesToUpdate.addAll(DBUtils.readIntegersToSet(psAddressesNoWallet.setMaxRows(readRowsBatch)));
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

    private void processAddress(int addressId, DbUpdateWallet addWallet, DbUpdateAddress updateAddress) throws SQLException, InterruptedException {
        Set<Integer> relatedWallets = new HashSet<>();
        IntCollection zeroWalletAddresses = new IntCollection(10000);
        AtomicInteger counter = new AtomicInteger();
        psRelatedAddresses.setParameters(p -> p.setInt(addressId)).setFetchSize(Integer.MIN_VALUE).executeQuery(rs -> {
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
            if (counter.incrementAndGet() % 1000000 == 0) {
                log.debug("Loading addresses from DB: {} M", counter.get() / 1000000);
            }
        });
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
        return DBUtils.readIntegersToSet(psQuerySpendTransactionsByAddress.setParameters(p -> p.setInt(addressId)));
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
                int nUpdated = psUpdateAddressWalletPerTable.stream().map(ps -> ps.setParameters(p -> p.setInt(walletId).setInt(a)).executeUpdate()).reduce(0, Integer::sum);
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
            psRelatedWallets.setParameters(p -> p.setInt(walletId).setInt(walletId).setInt(walletId).setInt(walletId)).executeQuery(rs -> {
                Integer w = rs.getObject(1, Integer.class);
                if (w == null) {
                    log.warn("Unexpected: wallet_id is null. Original wallet ID: {}", walletId);
                } else {
                    if (w != 0) {
                        relatedWallets.add(w);
                    }
                }
            });
        } else {
            //if wallet has too many addresses - run query for one address at a time
            List<Callable<Boolean>> todo = new ArrayList<>(addr.size());
            for (Integer addressId : addr) {
                todo.add(() -> {
                    psRelatedWalletsByAddress.setParameters(p -> p.setInt(addressId)).executeQuery(rs -> {
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
                    });
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
        return DBUtils.readIntegersToSet(psQueryAddressIdsByWallet.setParameters(p -> p.setInt(walletId).setInt(walletId).setInt(walletId).setInt(walletId)));
    }

    private int fixWalletsByTransactions(int minTransactionId, int maxTransactionId) throws SQLException, InterruptedException {
        log.debug("fixWalletsByTransactions(minTransactionId:{}, maxTransactionId:{})", minTransactionId, maxTransactionId);
        final AtomicInteger result = new AtomicInteger(0);
        for (Integer transactionId : DBUtils.readIntegersToSet(psQueryTransactionsWithDifferentInputWallets.setParameters(p -> p.setInt(minTransactionId).setInt(maxTransactionId)))) {
            log.debug("Fixing transactionId: {}", transactionId);
            int nUpdated = fixTransaction(transactionId);
            result.addAndGet(nUpdated);
        }
        return result.get();
    }

    private int fixTransaction(int transactionId) throws SQLException, InterruptedException {
        Collection<Integer> wList = DBUtils.readIntegersToSet(psQueryWalletdByTransaction.setParameters(p -> p.setInt(transactionId)));
        if (wList.size() < 2) {
            log.trace("Transaction {} has been fixed already: {}", transactionId, wList);
            return 0;
        }
        int minWalletId = wList.stream().min((i, j) -> Integer.compare(i, j)).get();
        log.debug("fixTransaction(transactionId:{}): minWalletId={}, wList={}", transactionId, minWalletId, wList);
        return processWallet(minWalletId, wList);
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

    private boolean isWalletUsed(int walletId) throws SQLException {
        return psQueryAllAddressesByWallet.setParameters(p -> p.setInt(walletId).setInt(walletId).setInt(walletId).setInt(walletId)).setMaxRows(1).querySingleRow(rs -> true).orElse(false);
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
        options.addOption(null, "address-type", true, "Addresses type to scan and update. Acceptable values: " + Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).collect(Collectors.toSet()) + ". Default: " + DEFAULT_ADDRESS_TYPE);
        options.addOption(null, "batch-size", true, "Number or addresses to read in a batch. Default: " + DEFAULT_READ_ROWS_BATCH);
        options.addOption(null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end. Default: " + DEFAULT_STOP_FILE_NAME);
        options.addOption("t", "threads", true, "Number of threads to run. Default is " + DEFAULT_TXN_THREADS + ". To disable parallel threading set value to 0");
        DBConnectionSupplier.addOptions(options);
        return options;
    }

}
