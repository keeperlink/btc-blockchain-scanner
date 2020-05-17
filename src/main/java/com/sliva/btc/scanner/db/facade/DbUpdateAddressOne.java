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
package com.sliva.btc.scanner.db.facade;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.DbUpdate;
import static com.sliva.btc.scanner.db.facade.DbQueryAddressOne.getAddressTableName;
import static com.sliva.btc.scanner.db.facade.DbQueryAddressOne.updateQueryTableName;
import com.sliva.btc.scanner.db.model.BinaryAddress;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Updater for one specific address table.
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateAddressOne extends DbUpdate {

    private static final String SQL_ADD = "INSERT INTO `address_table_name`(address_id,`address`,wallet_id)VALUES(?,?,?)";
    private static final String SQL_ADD_NO_WALLET_ID = "INSERT INTO `address_table_name`(address_id,`address`)VALUES(?,?)";
    private static final String SQL_UPDATE_WALLET = "UPDATE `address_table_name` SET wallet_id=? WHERE address_id=?";
    private final DBPreparedStatement psAdd;
    private final DBPreparedStatement psUpdateWallet;
    @Getter
    @NonNull
    private final CacheData cacheData;
    private final boolean hasWalletIdField;

    public DbUpdateAddressOne(DBConnectionSupplier conn, SrcAddressType addressType) {
        this(conn, addressType, new CacheData());
    }

    public DbUpdateAddressOne(DBConnectionSupplier conn, SrcAddressType addressType, CacheData cacheData) {
        super(getAddressTableName(addressType), conn);
        checkArgument(addressType.isReal(), "Argument 'addressType' is not a real type: %s", addressType);
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        this.hasWalletIdField = conn.getDBMetaData().hasField(getTableName() + ".wallet_id");
        this.psAdd = conn.prepareStatement(updateQueryTableName(hasWalletIdField ? SQL_ADD : SQL_ADD_NO_WALLET_ID, addressType));
        this.psUpdateWallet = hasWalletIdField
                ? conn.prepareStatement(updateQueryTableName(SQL_UPDATE_WALLET, addressType), getTableName() + ".address_id")
                : conn.prepareNonExecutableStatement(updateQueryTableName(SQL_UPDATE_WALLET, addressType), "Table " + getTableName() + " does not have field \"wallet_id\"");
        this.cacheData = cacheData;
    }

    @Override
    public int getCacheFillPercent() {
        return Math.max(cacheData.addQueue.size() * 100 / getMaxInsertsQueueSize(), cacheData.updateWalletQueue.size() * 100 / getMaxUpdatesQueueSize());
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData.addQueue.size() >= getMinBatchSize() || cacheData.updateWalletQueue.size() >= getMinBatchSize();
    }

    public void add(BtcAddress addr) {
        log.trace("add(): addr={}", addr);
        checkState(isActive(), "Instance has been closed");
        synchronized (cacheData) {
            if (!cacheData.addMap.containsKey(addr.getAddress()) && !cacheData.addMapId.containsKey(addr.getAddressId())) {
                cacheData.addMap.put(addr.getAddress(), addr);
                cacheData.addMapId.put(addr.getAddressId(), addr);
                cacheData.addQueue.add(addr);
            } else {
                log.debug("add(): Address already in the queue: addr={} addMap={}, addMapId={}",
                        addr, cacheData.addMap.get(addr.getAddress()), cacheData.addMapId.get(addr.getAddressId()));
            }
        }
        waitFullQueue(cacheData.addQueue, getMaxInsertsQueueSize());
    }

    public void updateWallet(BtcAddress btcAddress) {
        checkState(isActive(), "Instance has been closed");
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
        waitFullQueue(cacheData.updateWalletQueue, getMaxUpdatesQueueSize());
    }

    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, getMaxBatchSize(),
                (t, p) -> p.setInt(t.getAddressId()).setBytes(t.getAddress().getData()).ignoreExtraParam().setInt(t.getWalletId()),
                executed -> {
                    synchronized (cacheData) {
                        executed.stream()
                                .peek(t -> cacheData.addMap.remove(t.getAddress()))
                                .map(BtcAddress::getAddressId).forEach(cacheData.addMapId::remove);
                    }
                });
    }

    @Override
    public int executeUpdates() {
        return _executeUpdateWallet();
    }

    private int _executeUpdateWallet() {
        return executeBatch(cacheData, cacheData.updateWalletQueue, psUpdateWallet, getMaxBatchSize(),
                (t, p) -> p.setInt(t.getWalletId()).setInt(t.getAddressId()), null);
    }

    @Getter
    public static class CacheData {

        private final Collection<BtcAddress> addQueue = new LinkedHashSet<>();
        private final Map<BinaryAddress, BtcAddress> addMap = new HashMap<>();
        private final Map<Integer, BtcAddress> addMapId = new HashMap<>();
        private final Collection<BtcAddress> updateWalletQueue = new ArrayList<>();
    }
}
