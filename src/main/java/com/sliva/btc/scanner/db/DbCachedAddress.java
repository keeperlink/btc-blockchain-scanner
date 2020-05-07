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
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.Utils;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bitcoinj.core.Address;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedAddress implements AutoCloseable {

    private final Map<SrcAddressType, DbCachedAddressOne> updaters = new HashMap<>();
    private final CacheData cacheData;

    public DbCachedAddress(DBConnection conn) throws SQLException {
        this(conn, new CacheData());
    }

    public DbCachedAddress(DBConnection conn, CacheData cacheData) {
        this.cacheData = cacheData;
        BtcAddress.getRealTypes().forEach((t) -> updaters.put(t, new DbCachedAddressOne(conn, t, cacheData.dataOneMap.get(t))));
    }

    public CacheData getCacheData() {
        return cacheData;
    }

    @SuppressWarnings("DoubleCheckedLocking")
    public int getOrAdd(SrcAddress address, boolean updateCache) throws SQLException {
        checkArgument(address != null, "Argument 'address' is null");
        return getOne(BtcAddress.builder()
                .type(address.getType()).build())
                .getOrAdd(address.getHash(), updateCache);
    }

    public BtcAddress add(BtcAddress btcAddress, boolean updateCache) throws SQLException {
        return getOne(btcAddress).add(btcAddress, updateCache);
    }

    public BtcAddress getAddress(int addressId, boolean updateCache) throws SQLException {
        return getOne(BtcAddress.builder().addressId(addressId).build()).getAddress(addressId, updateCache);
    }

    public BtcAddress getAddress(Address address, boolean updateCache) throws SQLException {
        return getOne(BtcAddress.builder()
                .type(Utils.getBtcAddressType(address.getOutputScriptType()))
                .build()).getAddress(address.getHash(), updateCache);
    }

    private DbCachedAddressOne getOne(BtcAddress addr) {
        final SrcAddressType addrType = addr.getType();
        if (!BtcAddress.getRealTypes().contains(addrType)) {
            throw new IllegalArgumentException("Bad address type: " + addrType + ", addr=" + addr);
        }
        return updaters.get(addr.getType());
    }

    @Override
    public void close() {
        updaters.values().forEach((updater) -> updater.close());
    }

    @Getter
    public static class CacheData {

        private final Map<SrcAddressType, DbCachedAddressOne.CacheData> dataOneMap = new HashMap<>();

        public CacheData() {
            BtcAddress.getRealTypes().forEach((t) -> dataOneMap.put(t, new DbCachedAddressOne.CacheData()));
        }
    }
}
