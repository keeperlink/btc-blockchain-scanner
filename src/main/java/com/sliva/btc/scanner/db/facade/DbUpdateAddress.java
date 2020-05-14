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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateAddress implements AutoCloseable {

    private final Map<SrcAddressType, DbUpdateAddressOne> updaters = new HashMap<>();

    public DbUpdateAddress(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbUpdateAddress(DBConnectionSupplier conn, CacheData cacheData) {
        Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).forEach((t) -> updaters.put(t, new DbUpdateAddressOne(conn, t, cacheData.dataOneMap.get(t))));
    }

    public void add(BtcAddress addr) throws SQLException {
        checkArgument(addr != null, "Argument 'addr' is null");
        log.trace("add(): addr={}, type={}", addr, addr.getType());
        getUpdater(addr).add(addr);
    }

    public void updateWallet(int addressId, int walletId) {
        checkArgument(addressId > 0, "Argument 'addressId' must be a positive number: %s", addressId);
        try {
            updateWallet(BtcAddress.builder().addressId(addressId).walletId(walletId).build());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateWallet(BtcAddress addr) throws SQLException {
        checkArgument(addr != null, "Argument 'addr' is null");
        SrcAddressType addrType = addr.getType();
        checkArgument(addrType.isReal(), "Bad address type: %s, addr=%s", addrType, addr);
        log.trace("updateWallet(): addr={}, type={}", addr, addrType);
        getUpdater(addr).updateWallet(addr);
    }

    private DbUpdateAddressOne getUpdater(BtcAddress addr) {
        checkArgument(addr != null, "Argument 'addr' is null");
        SrcAddressType addrType = addr.getType();
        checkArgument(addrType.isReal(), "Bad address type: %s, addr=%s", addrType, addr);
        return updaters.get(addr.getType());
    }

    public void flushCache() {
        updaters.values().forEach(DbUpdateAddressOne::flushCache);
    }

    @Override
    public void close() {
        updaters.values().forEach(DbUpdateAddressOne::close);
    }

    @Getter
    public static class CacheData {

        private final Map<SrcAddressType, DbUpdateAddressOne.CacheData> dataOneMap = new HashMap<>();

        public CacheData() {
            Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).forEach((t) -> dataOneMap.put(t, new DbUpdateAddressOne.CacheData()));
        }
    }
}
