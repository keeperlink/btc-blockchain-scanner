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
import com.sliva.btc.scanner.src.SrcAddressType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;

/**
 * Updater for one specific address table.
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateAddressOne extends DbUpdate {

    public static int MIN_BATCH_SIZE = 1;
    public static int MAX_BATCH_SIZE = 40000;
    public static int MAX_INSERT_QUEUE_LENGTH = 1000000;
    private static int MAX_UPDATE_QUEUE_LENGTH = 1000;
    private static final String SQL_ADD = "INSERT INTO address_table_name(address_id,address,wallet_id)VALUES(?,?,?)";
    private static final String SQL_UPDATE_WALLET = "UPDATE address_table_name SET wallet_id=? WHERE address_id=?";
    private final SrcAddressType addressType;
    private final ThreadLocal<PreparedStatement> psAdd;
    private final ThreadLocal<PreparedStatement> psUpdateWallet;
    private final CacheData cacheData;

    public DbUpdateAddressOne(DBConnection conn, SrcAddressType addressType) throws SQLException {
        this(conn, addressType, new CacheData());
    }

    public DbUpdateAddressOne(DBConnection conn, SrcAddressType addressType, CacheData cacheData) {
        super(conn);
        this.addressType = addressType;
        this.psAdd = conn.prepareStatement(fixTableName(SQL_ADD));
        this.psUpdateWallet = conn.prepareStatement(fixTableName(SQL_UPDATE_WALLET));
        this.cacheData = cacheData;
    }

    public CacheData getCacheData() {
        return cacheData;
    }

    @Override
    public String getTableName() {
        return "address_" + addressType.name().toLowerCase();
    }

    @Override
    public int getCacheFillPercent() {
        return cacheData == null ? 0 : cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH;
    }

    @Override
    public boolean needExecuteInserts() {
        return cacheData == null ? false : cacheData.addQueue.size() >= MIN_BATCH_SIZE;
    }

    public void add(BtcAddress addr) throws SQLException {
        log.trace("add(): addr={}", addr);
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        synchronized (cacheData) {
            String hexAddr = Hex.toHexString(addr.getAddress());
            if (!cacheData.addMap.containsKey(hexAddr) && !cacheData.addMapId.containsKey(addr.getAddressId())) {
                cacheData.addMap.put(hexAddr, addr);
                cacheData.addMapId.put(addr.getAddressId(), addr);
                cacheData.addQueue.add(addr);
            } else {
                log.debug("add(): Address already in the queue: addr={} addMap={}, addMapId={}",
                        addr, cacheData.addMap.get(hexAddr), cacheData.addMapId.get(addr.getAddressId()));
            }
        }
    }

    public void updateWallet(BtcAddress btcAddress) throws SQLException {
        synchronized (cacheData) {
            BtcAddress a = cacheData.addMapId.get(btcAddress.getAddressId());
            boolean updatedInQueue = false;
            if (a != null) {
                if (a.getWalletId() != btcAddress.getWalletId()) {
                    if (cacheData.addQueue.remove(a)) {
                        cacheData.addQueue.add(a.toBuilder().walletId(btcAddress.getWalletId()).build());
                        updatedInQueue = true;
                    }
                } else {
                    //values not changed
                    updatedInQueue = true;
                }
            }
            if (!updatedInQueue) {
                cacheData.updateWalletQueue.add(btcAddress);
            }
        }
        if (cacheData.updateWalletQueue.size() >= MAX_UPDATE_QUEUE_LENGTH) {
            executeUpdateWallet();
        }
    }

    @Override
    public int executeInserts() {
        Collection<BtcAddress> temp = null;
        synchronized (cacheData) {
            if (!cacheData.addQueue.isEmpty()) {
                temp = new ArrayList<>();
                Iterator<BtcAddress> it = cacheData.addQueue.iterator();
                for (int i = 0; i < MAX_BATCH_SIZE && it.hasNext(); i++) {
                    temp.add(it.next());
                    it.remove();
                }
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                BatchExecutor.executeBatch(temp, psAdd.get(), (t, ps) -> {
                    ps.setInt(1, t.getAddressId());
                    ps.setBytes(2, t.getAddress());
                    ps.setInt(3, t.getWalletId());
                });
                synchronized (cacheData) {
                    for (BtcAddress t : temp) {
                        cacheData.addMap.remove(Hex.toHexString(t.getAddress()));
                        cacheData.addMapId.remove(t.getAddressId());
                    }
                }
            }
        }
        return temp == null ? 0 : temp.size();
    }

    public void executeUpdateWallet() {
        Collection<BtcAddress> temp = null;
        synchronized (cacheData) {
            if (!cacheData.updateWalletQueue.isEmpty()) {
                temp = new ArrayList<>(cacheData.updateWalletQueue);
                cacheData.updateWalletQueue.clear();
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                BatchExecutor.executeBatch(temp, psUpdateWallet.get(), (BtcAddress t, PreparedStatement ps) -> {
                    ps.setInt(1, t.getWalletId());
                    ps.setInt(2, t.getAddressId());
                });
            }
        }
    }

    @Override
    public void flushCache() {
        log.trace("{}.flushCache() Called", getTableName());
        super.flushCache();
        executeUpdateWallet();
    }

    @Override
    public void close() {
        super.close();
        executeUpdateWallet();
    }

    private String fixTableName(String sql) {
        return sql.replaceAll("address_table_name", getTableName());
    }

    @Getter
    public static class CacheData {

        private final Collection<BtcAddress> addQueue = new LinkedHashSet<>();
        private final Map<String, BtcAddress> addMap = new HashMap<>();
        private final Map<Integer, BtcAddress> addMapId = new HashMap<>();
        private final Collection<BtcAddress> updateWalletQueue = new ArrayList<>();
    }
}
