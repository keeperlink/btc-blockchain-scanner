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

import com.sliva.btc.scanner.db.model.BtcWallet;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbAddWallet extends DbUpdate {

    private static int MIN_BATCH_SIZE = 1;
    public static int MAX_BATCH_SIZE = 1000;
    private static int MAX_INSERT_QUEUE_LENGTH = 1000;
    private static final String TABLE_NAME = "wallet";
    private static final String SQL = "INSERT INTO wallet(wallet_id,name,details)VALUES(?,?,?)";
    private final ThreadLocal<PreparedStatement> psAdd;
    private final CacheData cacheData;

    public DbAddWallet(DBConnection conn) {
        this(conn, new CacheData());
    }

    public DbAddWallet(DBConnection conn, CacheData cacheData) {
        super(conn);
        this.psAdd = conn.prepareStatement(SQL);
        this.cacheData = cacheData;
    }

    public CacheData getCacheData() {
        return cacheData;
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public int getCacheFillPercent() {
        return cacheData == null ? 0 : cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH;
    }

    @Override
    public boolean needExecuteInserts() {
        return cacheData == null ? false : cacheData.addQueue.size() >= MIN_BATCH_SIZE;
    }

    public BtcWallet add(BtcWallet wallet) throws SQLException {
        log.trace("add(wallet:{})", wallet);
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        if (wallet == null) {
            wallet = BtcWallet.builder().walletId(getNextWalletId()).build();
        } else if (wallet.getWalletId() == 0) {
            wallet = wallet.toBuilder().walletId(getNextWalletId()).build();
        }
        synchronized (cacheData) {
            cacheData.addQueue.add(wallet);
        }
        return wallet;
    }

    public void add(int walletId) {
        try {
            add(BtcWallet.builder().walletId(walletId).build());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    @Override
    public int executeInserts() {
        Collection<BtcWallet> temp = null;
        synchronized (cacheData) {
            if (!cacheData.addQueue.isEmpty()) {
                temp = new ArrayList<>();
                Iterator<BtcWallet> it = cacheData.addQueue.iterator();
                for (int i = 0; i < MAX_BATCH_SIZE && it.hasNext(); i++) {
                    temp.add(it.next());
                    it.remove();
                }
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                BatchExecutor.executeBatch(temp, psAdd.get(), (BtcWallet t, PreparedStatement ps) -> {
                    ps.setInt(1, t.getWalletId());
                    ps.setString(2, t.getName());
                    ps.setString(3, t.getDescription());
                });
            }
        }
        return temp == null ? 0 : temp.size();
    }

    private int getNextWalletId() throws SQLException {
        synchronized (cacheData.lastWalletId) {
            if (cacheData.lastWalletId.get() == 0) {
                cacheData.lastWalletId.set(new DbQueryWallet(getConn()).getMaxId());
            }
            return cacheData.lastWalletId.incrementAndGet();
        }
    }

    @Getter
    public static class CacheData {

        private final Collection<BtcWallet> addQueue = new ArrayList<>();
        private final AtomicInteger lastWalletId = new AtomicInteger(0);
    }
}
