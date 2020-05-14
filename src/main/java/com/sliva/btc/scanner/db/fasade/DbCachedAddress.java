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
package com.sliva.btc.scanner.db.fasade;

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.Utils;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bitcoinj.core.Address;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedAddress implements AutoCloseable {

    private final Map<SrcAddressType, DbCachedAddressOne> updaters = new HashMap<>();

    public DbCachedAddress(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbCachedAddress(DBConnectionSupplier conn, CacheData cacheData) {
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal)
                .forEach((t) -> updaters.put(t, new DbCachedAddressOne(conn, t, cacheData.dataOneMap.get(t))));
    }

    @SuppressWarnings("DoubleCheckedLocking")
    @SneakyThrows(SQLException.class)
    public int getOrAdd(SrcAddress address, boolean updateCache) {
        checkArgument(address != null, "Argument 'address' is null");
        return getOne(BtcAddress.builder()
                .type(address.getType()).build())
                .getOrAdd(address.getHash(), updateCache);
    }

    @NonNull
    public BtcAddress add(BtcAddress btcAddress, boolean updateCache) {
        return getOne(btcAddress).add(btcAddress, updateCache);
    }

    @NonNull
    public Optional<BtcAddress> getAddress(int addressId, boolean updateCache) {
        return getOne(BtcAddress.builder().addressId(addressId).build()).getAddress(addressId, updateCache);
    }

    @NonNull
    public Optional<BtcAddress> getAddress(Address address, boolean updateCache) {
        return getOne(BtcAddress.builder()
                .type(Utils.getBtcAddressType(address.getOutputScriptType()))
                .build()).getAddress(address.getHash(), updateCache);
    }

    private DbCachedAddressOne getOne(BtcAddress addr) {
        final SrcAddressType addrType = addr.getType();
        if (!addrType.isReal()) {
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
            Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).forEach((t) -> dataOneMap.put(t, new DbCachedAddressOne.CacheData()));
        }
    }
}
