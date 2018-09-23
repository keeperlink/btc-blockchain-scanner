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
package com.sliva.btc.scanner.db;

import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcWallet;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Sliva Co
 */
public class DbQueryWallet {

    private static final String SQL_QUERY_WALLET
            = "SELECT name,details FROM wallet WHERE wallet_id=?";
    private static final String SQL_MAX_ID
            = "SELECT wallet_id FROM wallet ORDER BY wallet_id DESC LIMIT 1";
    private static final String SQL_QUERY_WALLET_ADDRESSES
            = "SELECT address_id,address FROM address_p2pkh WHERE wallet_id=?"
            + " UNION SELECT address_id,address FROM address_p2sh WHERE wallet_id=?"
            + " UNION SELECT address_id,address FROM address_p2wpkh WHERE wallet_id=?"
            + " UNION SELECT address_id,CAST(address AS BINARY) FROM address_p2wsh WHERE wallet_id=?";
    private static final String SQL_QUERY_MISSING_WALLETS
            = "SELECT wallet_id FROM address_p2pkh WHERE wallet_id>0 AND wallet_id NOT IN (SELECT wallet_id FROM wallet)"
            + " UNION SELECT wallet_id FROM address_p2sh WHERE wallet_id>0 AND wallet_id NOT IN (SELECT wallet_id FROM wallet)"
            + " UNION SELECT wallet_id FROM address_p2wpkh WHERE wallet_id>0 AND wallet_id NOT IN (SELECT wallet_id FROM wallet)"
            + " UNION SELECT wallet_id FROM address_p2wsh WHERE wallet_id>0 AND wallet_id NOT IN (SELECT wallet_id FROM wallet)";
    private static final String SQL_QUERY_MISSING_WALLETS_IN_RANGE
            = "SELECT wallet_id FROM address_p2pkh WHERE wallet_id BETWEEN ? AND ? AND wallet_id NOT IN (SELECT wallet_id FROM wallet WHERE wallet_id BETWEEN ? AND ?)"
            + " UNION SELECT wallet_id FROM address_p2sh WHERE wallet_id BETWEEN ? AND ? AND wallet_id NOT IN (SELECT wallet_id FROM wallet WHERE wallet_id BETWEEN ? AND ?)"
            + " UNION SELECT wallet_id FROM address_p2wpkh WHERE wallet_id BETWEEN ? AND ? AND wallet_id NOT IN (SELECT wallet_id FROM wallet WHERE wallet_id BETWEEN ? AND ?)"
            + " UNION SELECT wallet_id FROM address_p2wsh WHERE wallet_id BETWEEN ? AND ? AND wallet_id NOT IN (SELECT wallet_id FROM wallet WHERE wallet_id BETWEEN ? AND ?)";
    private static final String SQL_QUERY_UNUSED_WALLETS
            = "SELECT wallet_id FROM wallet"
            + " WHERE wallet_id NOT IN (SELECT wallet_id FROM address_p2pkh WHERE wallet_id>0)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2sh WHERE wallet_id>0)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2wpkh WHERE wallet_id>0)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2wsh WHERE wallet_id>0)";
    private static final String SQL_QUERY_UNUSED_WALLETS_IN_RANGE
            = "SELECT wallet_id FROM wallet"
            + " WHERE wallet_id BETWEEN ? AND ?"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2pkh WHERE wallet_id BETWEEN ? AND ?)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2sh WHERE wallet_id BETWEEN ? AND ?)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2wpkh WHERE wallet_id BETWEEN ? AND ?)"
            + " AND wallet_id NOT IN (SELECT wallet_id FROM address_p2wsh WHERE wallet_id BETWEEN ? AND ?)";
    private final ThreadLocal<PreparedStatement> psQueryWallet;
    private final ThreadLocal<PreparedStatement> psMaxId;
    private final ThreadLocal<PreparedStatement> psQueryWalletAddresses;
    private final ThreadLocal<PreparedStatement> psQueryMissingWallets;
    private final ThreadLocal<PreparedStatement> psQueryMissingWalletsInRange;
    private final ThreadLocal<PreparedStatement> psQueryUnusedWallets;
    private final ThreadLocal<PreparedStatement> psQueryUnusedWalletsInRange;

    public DbQueryWallet(DBConnection conn) {
        this.psQueryWallet = conn.prepareStatement(SQL_QUERY_WALLET);
        this.psMaxId = conn.prepareStatement(SQL_MAX_ID);
        this.psQueryWalletAddresses = conn.prepareStatement(SQL_QUERY_WALLET_ADDRESSES);
        this.psQueryMissingWallets = conn.prepareStatement(SQL_QUERY_MISSING_WALLETS);
        this.psQueryMissingWalletsInRange = conn.prepareStatement(SQL_QUERY_MISSING_WALLETS_IN_RANGE);
        this.psQueryUnusedWallets = conn.prepareStatement(SQL_QUERY_UNUSED_WALLETS);
        this.psQueryUnusedWalletsInRange = conn.prepareStatement(SQL_QUERY_UNUSED_WALLETS_IN_RANGE);
    }

    public BtcWallet getWallet(int walletId) throws SQLException {
        psQueryWallet.get().setInt(1, walletId);
        try (ResultSet rs = psQueryWallet.get().executeQuery()) {
            return rs.next() ? BtcWallet.builder()
                    .walletId(walletId)
                    .name(rs.getString(1))
                    .description(rs.getString(2))
                    .build() : null;
        }
    }

    public int getMaxId() throws SQLException {
        try (ResultSet rs = psMaxId.get().executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public Collection<BtcAddress> getWalletAddresses(int walletId) throws SQLException {
        psQueryWalletAddresses.get().setInt(1, walletId);
        psQueryWalletAddresses.get().setInt(2, walletId);
        psQueryWalletAddresses.get().setInt(3, walletId);
        psQueryWalletAddresses.get().setInt(4, walletId);
        try (ResultSet rs = psQueryWalletAddresses.get().executeQuery()) {
            Collection<BtcAddress> result = new ArrayList<>();
            while (rs.next()) {
                result.add(BtcAddress.builder()
                        .addressId(rs.getInt(1))
                        .address(rs.getBytes(2))
                        .walletId(walletId)
                        .build());
            }
            return result;
        }
    }

    public Collection<Integer> getMissingWallets() throws SQLException {
        return DBUtils.readIntegersToSet(psQueryMissingWallets.get());
    }

    public Collection<Integer> getMissingWalletsInRange(int minWalletId, int maxWalletId) throws SQLException {
        for (int i = 1; i <= 8; i++) {
            psQueryMissingWalletsInRange.get().setInt(i * 2 - 1, minWalletId);
            psQueryMissingWalletsInRange.get().setInt(i * 2, maxWalletId);
        }
        return DBUtils.readIntegersToSet(psQueryMissingWalletsInRange.get());
    }

    public Collection<Integer> getMissingWalletsParallel() throws SQLException, InterruptedException {
        final Collection<Integer> result = new HashSet<>();
        final int maxId = getMaxId();
        final int numThreads = 10;
        ExecutorService exec = Executors.newFixedThreadPool(numThreads);
        final int step = maxId / numThreads + 1;
        for (int i = 1; i <= maxId; i += step) {
            final int minWalletId = i, maxWalletId = i + step - 1;
            exec.submit(() -> {
                try {
                    Collection<Integer> t = getMissingWalletsInRange(minWalletId, maxWalletId);
                    synchronized (result) {
                        result.addAll(t);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.HOURS);
        return result;
    }

    public Collection<Integer> getUnusedWalletRecords() throws SQLException {
        return DBUtils.readIntegersToSet(psQueryUnusedWallets.get());
    }

    public Collection<Integer> getUnusedWalletRecordsInRange(int minWalletId, int maxWalletId) throws SQLException {
        for (int i = 1; i <= 5; i++) {
            psQueryUnusedWalletsInRange.get().setInt(i * 2 - 1, minWalletId);
            psQueryUnusedWalletsInRange.get().setInt(i * 2, maxWalletId);
        }
        return DBUtils.readIntegersToSet(psQueryUnusedWalletsInRange.get());
    }

    public Collection<Integer> getUnusedWalletRecordsParallel() throws SQLException, InterruptedException {
        final Collection<Integer> result = new HashSet<>();
        final int maxId = getMaxId();
        final int numThreads = 10;
        ExecutorService exec = Executors.newFixedThreadPool(numThreads);
        final int step = maxId / numThreads + 1;
        for (int i = 1; i <= maxId; i += step) {
            final int minWalletId = i, maxWalletId = i + step - 1;
            exec.submit(() -> {
                try {
                    Collection<Integer> t = getUnusedWalletRecordsInRange(minWalletId, maxWalletId);
                    synchronized (result) {
                        result.addAll(t);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.HOURS);
        return result;
    }
}
