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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedAddressOne implements AutoCloseable {

    private static final int MAX_CACHE_SIZE = 100000;
    private final SrcAddressType addressType;
    private final DbUpdateAddressOne updateAddress;
    private final DbQueryAddress queryAddress;
    private final CacheData cacheData;

    public DbCachedAddressOne(DBConnectionSupplier conn, SrcAddressType addressType) {
        this(conn, addressType, new CacheData());
    }

    public DbCachedAddressOne(DBConnectionSupplier conn, SrcAddressType addressType, CacheData cacheData) {
        this.addressType = addressType;
        updateAddress = new DbUpdateAddressOne(conn, addressType, cacheData.updateCachedData);
        queryAddress = new DbQueryAddress(conn, addressType);
        this.cacheData = cacheData;
    }

    @NonNull
    public CacheData getCacheData() {
        return cacheData;
    }

    public int getOrAdd(byte[] address, boolean updateCache) throws SQLException {
        return getAddress(address, updateCache).orElseGet(() -> _getOrAddSync(address, updateCache)).getAddressId();
    }

    @NonNull
    private BtcAddress _getOrAddSync(byte[] address, boolean updateCache) {
        synchronized (cacheData) {
            return _getOrAddNotSync(address, updateCache);
        }
    }

    @NonNull
    private BtcAddress _getOrAddNotSync(byte[] address, boolean updateCache) {
        BtcAddress a = cacheData.cacheMap.get(Hex.toHexString(address));
        if (a == null) {
            a = add(BtcAddress.builder().type(addressType).address(address).build(), updateCache);
        }
        return a;
    }

    @NonNull
    @SneakyThrows(SQLException.class)
    public BtcAddress add(BtcAddress btcAddress, boolean updateCache) {
        BtcAddress a = btcAddress;
        if (a.getAddressId() == 0) {
            a = a.toBuilder().addressId(getNextAddressId()).build();
        }
        if (updateCache) {
            updateCache(a);
        }
        updateAddress.add(a);
        return a;
    }

    @NonNull
    public Optional<BtcAddress> getAddress(int addressId, boolean updateCache) {
        Optional<BtcAddress> result = Optional.ofNullable(cacheData.cacheMapId.get(addressId));
        if (!result.isPresent()) {
            result = Optional.ofNullable(updateAddress.getCacheData().getAddMapId().get(addressId));
        }
        if (!result.isPresent()) {
            result = queryAddress.findByAddressId(addressId);
        }
        if (updateCache) {
            result.ifPresent(r -> updateCache(r));
        }
        return result;
    }

    @NonNull
    public Optional<BtcAddress> getAddress(byte[] address, boolean updateCache) {
        String hexAddr = Hex.toHexString(address);
        Optional<BtcAddress> result = Optional.ofNullable(cacheData.cacheMap.get(hexAddr));
        if (!result.isPresent()) {
            result = Optional.ofNullable(updateAddress.getCacheData().getAddMap().get(hexAddr));
        }
        if (!result.isPresent()) {
            result = queryAddress.findByAddress(address);
        }
        if (updateCache) {
            result.ifPresent(r -> updateCache(r));
        }
        return result;
    }

    private int getNextAddressId() throws SQLException {
        synchronized (cacheData.lastAddressId) {
            if (cacheData.lastAddressId.get() == 0) {
                cacheData.lastAddressId.set(queryAddress.getLastAddressId());
            }
            return cacheData.lastAddressId.incrementAndGet();
        }
    }

    private void updateCache(BtcAddress btcAddress) {
        synchronized (cacheData) {
            String hexAddr = Hex.toHexString(btcAddress.getAddress());
            cacheData.cacheMap.remove(hexAddr);
            cacheData.cacheMap.put(hexAddr, btcAddress);
            cacheData.cacheMapId.put(btcAddress.getAddressId(), btcAddress);
            if (cacheData.cacheMap.size() >= MAX_CACHE_SIZE) {
                BtcAddress a = cacheData.cacheMap.entrySet().iterator().next().getValue();
                cacheData.cacheMap.remove(Hex.toHexString(a.getAddress()));
                cacheData.cacheMapId.remove(a.getAddressId());
            }
        }
    }

    @Override
    public void close() {
        log.debug("DbCachedAddressOne-{}.close()", addressType);
        synchronized (cacheData) {
            updateAddress.close();
        }
    }

    @Getter
    public static class CacheData {

        private final Map<String, BtcAddress> cacheMap = new LinkedHashMap<>();
        private final Map<Integer, BtcAddress> cacheMapId = new HashMap<>();
        private final AtomicInteger lastAddressId = new AtomicInteger(0);
        private final DbUpdateAddressOne.CacheData updateCachedData = new DbUpdateAddressOne.CacheData();
    }
}
