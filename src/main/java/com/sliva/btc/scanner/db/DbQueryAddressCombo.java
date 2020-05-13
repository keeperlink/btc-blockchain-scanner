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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.NonNull;

/**
 *
 * @author whost
 */
public class DbQueryAddressCombo extends DbQueryAddressOne {

    private final Map<SrcAddressType, DbQueryAddressOne> queryAddressMap = new HashMap<>();

    public DbQueryAddressCombo(DBConnectionSupplier conn) {
        super();
        Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).forEach(t -> queryAddressMap.put(t, new DbQueryAddressOne(conn, t)));
    }

    @Override
    @NonNull
    public Optional<BtcAddress> findByAddress(byte[] address) {
        return Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal)
                .filter(type -> address.length == 20 ^ type == SrcAddressType.P2WSH)
                .map(type -> queryAddressMap.get(type).findByAddress(address))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findAny();
    }

    @Override
    @NonNull
    public Optional<BtcAddress> findByAddressId(int addressId) {
        return queryAddressMap.get(BtcAddress.getTypeFromId(addressId)).findByAddressId(addressId);
    }

    @Override
    public int getLastAddressId() {
        throw new UnsupportedOperationException();
    }

    @Override
    @NonNull
    public Optional<Integer> getWalletId(int addressId) {
        return findByAddressId(addressId).map(BtcAddress::getWalletId);
    }

    @Override
    @NonNull
    public String getTableName() {
        return "";
    }
}
