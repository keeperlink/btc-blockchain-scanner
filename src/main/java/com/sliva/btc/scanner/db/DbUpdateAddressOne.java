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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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
import org.spongycastle.util.encoders.Hex;

/**
 * Updater for one specific address table.
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateAddressOne extends DbUpdate {

    private static int MIN_BATCH_SIZE = 1000;
    private static int MAX_BATCH_SIZE = 60000;
    private static int MAX_INSERT_QUEUE_LENGTH = 120000;
    private static int MAX_UPDATE_QUEUE_LENGTH = 10000;
    private static final String TABLE_NAME_TO_FILL = "__address_table_name__";
    private static final String SQL_ADD = "INSERT INTO __address_table_name__(address_id,address,wallet_id)VALUES(?,?,?)";
    private static final String SQL_UPDATE_WALLET = "UPDATE __address_table_name__ SET wallet_id=? WHERE address_id=?";
    private final SrcAddressType addressType;
    private final DBPreparedStatement psAdd;
    private final DBPreparedStatement psUpdateWallet;
    @Getter
    @NonNull
    private final CacheData cacheData;

    public DbUpdateAddressOne(DBConnectionSupplier conn, SrcAddressType addressType) {
        this(conn, addressType, new CacheData());
    }

    public DbUpdateAddressOne(DBConnectionSupplier conn, SrcAddressType addressType, CacheData cacheData) {
        super("address_" + checkNotNull(addressType, "Argument 'addressType' is null").name().toLowerCase(), conn);
        checkArgument(addressType.isReal(), "Argument 'addressType' is not a real type: %s", addressType);
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        this.addressType = addressType;
        this.psAdd = conn.prepareStatement(fixTableName(SQL_ADD));
        this.psUpdateWallet = conn.prepareStatement(fixTableName(SQL_UPDATE_WALLET));
        this.cacheData = cacheData;
    }

    @Override
    public int getCacheFillPercent() {
        return Math.max(cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH, cacheData.updateWalletQueue.size() * 100 / MAX_UPDATE_QUEUE_LENGTH);
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData.addQueue.size() >= MIN_BATCH_SIZE || cacheData.updateWalletQueue.size() >= MIN_BATCH_SIZE;
    }

    public void add(BtcAddress addr) {
        log.trace("add(): addr={}", addr);
        checkState(isActive(), "Instance has been closed");
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
        if (cacheData.updateWalletQueue.size() >= MAX_UPDATE_QUEUE_LENGTH) {
            _executeUpdateWallet();
        }
    }

    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, MAX_BATCH_SIZE,
                (t, p) -> p.setInt(t.getAddressId()).setBytes(t.getAddress()).setInt(t.getWalletId()),
                executed -> {
                    synchronized (cacheData) {
                        executed.stream()
                                .peek(t -> cacheData.addMap.remove(Hex.toHexString(t.getAddress())))
                                .map(BtcAddress::getAddressId).forEach(cacheData.addMapId::remove);
                    }
                });
    }

    @Override
    public int executeUpdates() {
        return _executeUpdateWallet();
    }

    private int _executeUpdateWallet() {
        return executeBatch(cacheData, cacheData.updateWalletQueue, psUpdateWallet, MAX_BATCH_SIZE,
                (t, p) -> p.setInt(t.getWalletId()).setInt(t.getAddressId()), null);
    }

    @NonNull
    private String fixTableName(String sql) {
        return sql.replaceAll(TABLE_NAME_TO_FILL, getTableName());
    }

    @Getter
    public static class CacheData {

        private final Collection<BtcAddress> addQueue = new LinkedHashSet<>();
        private final Map<String, BtcAddress> addMap = new HashMap<>();
        private final Map<Integer, BtcAddress> addMapId = new HashMap<>();
        private final Collection<BtcAddress> updateWalletQueue = new ArrayList<>();
    }
}
