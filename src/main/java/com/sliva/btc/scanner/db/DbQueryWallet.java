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

import com.sliva.btc.scanner.db.DBPreparedStatement.ParamSetter;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcWallet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.NonNull;

/**
 *
 * @author Sliva Co
 */
public class DbQueryWallet {

    private static final String SQL_QUERY_WALLET
            = "SELECT name,details FROM wallet WHERE wallet_id=? LIMIT 1";
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
    private final DBPreparedStatement psQueryWallet;
    private final DBPreparedStatement psMaxId;
    private final DBPreparedStatement psQueryWalletAddresses;
    private final DBPreparedStatement psQueryMissingWallets;
    private final DBPreparedStatement psQueryMissingWalletsInRange;
    private final DBPreparedStatement psQueryUnusedWallets;
    private final DBPreparedStatement psQueryUnusedWalletsInRange;

    public DbQueryWallet(DBConnectionSupplier conn) {
        this.psQueryWallet = conn.prepareStatement(SQL_QUERY_WALLET, "wallet.wallet_id");
        this.psMaxId = conn.prepareStatement(SQL_MAX_ID, "wallet.wallet_id");
        this.psQueryWalletAddresses = conn.prepareStatement(SQL_QUERY_WALLET_ADDRESSES, "address_p2pkh.wallet_id", "address_p2sh.wallet_id", "address_p2wpkh.wallet_id", "address_p2wsh.wallet_id");
        this.psQueryMissingWallets = conn.prepareStatement(SQL_QUERY_MISSING_WALLETS);
        this.psQueryMissingWalletsInRange = conn.prepareStatement(SQL_QUERY_MISSING_WALLETS_IN_RANGE, "address_p2pkh.wallet_id", "address_p2sh.wallet_id", "address_p2wpkh.wallet_id", "address_p2wsh.wallet_id");
        this.psQueryUnusedWallets = conn.prepareStatement(SQL_QUERY_UNUSED_WALLETS);
        this.psQueryUnusedWalletsInRange = conn.prepareStatement(SQL_QUERY_UNUSED_WALLETS_IN_RANGE, "address_p2pkh.wallet_id", "address_p2sh.wallet_id", "address_p2wpkh.wallet_id", "address_p2wsh.wallet_id");
    }

    @NonNull
    public Optional<BtcWallet> getWallet(int walletId) {
        return psQueryWallet
                .setParameters(p -> p.setInt(walletId))
                .querySingleRow(
                        rs -> BtcWallet.builder()
                                .walletId(walletId)
                                .name(rs.getString(1))
                                .description(rs.getString(2))
                                .build());
    }

    @NonNull
    public Optional<Integer> getMaxId() {
        return DBUtils.readInteger(psMaxId);
    }

    @NonNull
    public Collection<BtcAddress> getWalletAddresses(int walletId) {
        return psQueryWalletAddresses
                .setParameters(p -> p.setInt(walletId).setInt(walletId).setInt(walletId).setInt(walletId))
                .executeQueryToList(
                        rs -> BtcAddress.builder()
                                .addressId(rs.getInt(1))
                                .address(rs.getBytes(2))
                                .walletId(walletId)
                                .build());
    }

    @NonNull
    public Collection<Integer> getMissingWallets() {
        return DBUtils.readIntegersToSet(psQueryMissingWallets);
    }

    @NonNull
    public Collection<Integer> getMissingWalletsInRange(int minWalletId, int maxWalletId) {
        ParamSetter p = psQueryMissingWalletsInRange.getParamSetter();
        IntStream.range(0, 8).forEach(i -> p.setInt(minWalletId).setInt(maxWalletId));
        return DBUtils.readIntegersToSet(psQueryMissingWalletsInRange);
    }

    @NonNull
    public Collection<Integer> getMissingWalletsParallel() throws InterruptedException {
        final Collection<Integer> result = new HashSet<>();
        final int maxId = getMaxId().orElse(0);
        final int numThreads = 10;
        ExecutorService exec = Executors.newFixedThreadPool(numThreads);
        final int step = maxId / numThreads + 1;
        for (int i = 1; i <= maxId; i += step) {
            final int minWalletId = i, maxWalletId = i + step - 1;
            exec.submit(() -> {
                Collection<Integer> t = getMissingWalletsInRange(minWalletId, maxWalletId);
                synchronized (result) {
                    result.addAll(t);
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.HOURS);
        return result;
    }

    @NonNull
    public Collection<Integer> getUnusedWalletRecords() {
        return DBUtils.readIntegersToSet(psQueryUnusedWallets);
    }

    @NonNull
    public Collection<Integer> getUnusedWalletRecordsInRange(int minWalletId, int maxWalletId) {
        ParamSetter p = psQueryMissingWalletsInRange.getParamSetter();
        IntStream.range(0, 5).forEach(i -> p.setInt(minWalletId).setInt(maxWalletId));
        return DBUtils.readIntegersToSet(psQueryUnusedWalletsInRange);
    }

    @NonNull
    public Collection<Integer> getUnusedWalletRecordsParallel() throws InterruptedException {
        final Collection<Integer> result = new HashSet<>();
        final int maxId = getMaxId().orElse(0);
        final int numThreads = 10;
        ExecutorService exec = Executors.newFixedThreadPool(numThreads);
        final int step = maxId / numThreads + 1;
        for (int i = 1; i <= maxId; i += step) {
            final int minWalletId = i, maxWalletId = i + step - 1;
            exec.submit(() -> {
                Collection<Integer> t = getUnusedWalletRecordsInRange(minWalletId, maxWalletId);
                synchronized (result) {
                    result.addAll(t);
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.HOURS);
        return result;
    }
}
