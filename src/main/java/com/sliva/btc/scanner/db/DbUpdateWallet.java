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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.sliva.btc.scanner.db.model.BtcWallet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateWallet extends DbUpdate {

    private static int MIN_BATCH_SIZE = 1;
    private static int MAX_BATCH_SIZE = 5000;
    private static int MAX_INSERT_QUEUE_LENGTH = 2000;
    private static final String TABLE_NAME = "wallet";
    private static final String SQL_ADD = "INSERT INTO wallet(wallet_id,name,details)VALUES(?,?,?)";
    private final DBPreparedStatement psAdd;
    @Getter
    @NonNull
    private final CacheData cacheData;
    private final DbQueryWallet dbQueryWallet;

    public DbUpdateWallet(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbUpdateWallet(DBConnectionSupplier conn, CacheData cacheData) {
        super(TABLE_NAME, conn);
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        this.psAdd = conn.prepareStatement(SQL_ADD);
        this.cacheData = cacheData;
        this.dbQueryWallet = new DbQueryWallet(conn);
    }

    @Override
    public int getCacheFillPercent() {
        return cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH;
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData.addQueue.size() >= MIN_BATCH_SIZE;
    }

    @NonNull
    public BtcWallet add() throws SQLException {
        checkState(isActive(), "Instance has been closed");
        return add(getNextWalletId());
    }

    @NonNull
    public BtcWallet add(int walletId) {
        return add(BtcWallet.builder().walletId(walletId).build());
    }

    @NonNull
    public BtcWallet add(BtcWallet wallet) {
        checkArgument(wallet != null, "Argument 'wallet' is null");
        log.trace("add(wallet:{})", wallet);
        checkState(isActive(), "Instance has been closed");
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        BtcWallet wallet2 = wallet;
        if (wallet2.getWalletId() == 0) {
            wallet2 = wallet2.toBuilder().walletId(getNextWalletId()).build();
        }
        synchronized (cacheData) {
            cacheData.addQueue.add(wallet2);
        }
        return wallet;
    }

    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, MAX_BATCH_SIZE,
                (t, ps) -> ps.setInt(t.getWalletId()).setString(t.getName()).setString(t.getDescription()), null);
    }

    @Override
    public int executeUpdates() {
        return 0;
    }

    private int getNextWalletId() {
        synchronized (cacheData.lastWalletId) {
            if (cacheData.lastWalletId.get() == -1) {
                cacheData.lastWalletId.set(dbQueryWallet.getMaxId().orElse(0));
            }
            return cacheData.lastWalletId.incrementAndGet();
        }
    }

    @Getter
    public static class CacheData {

        private final Collection<BtcWallet> addQueue = new ArrayList<>();
        private final AtomicInteger lastWalletId = new AtomicInteger(-1);
    }
}
