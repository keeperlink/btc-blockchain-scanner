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
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.NonNull;
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
        checkArgument(conn != null, "Argument 'conn' is null");
        Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal)
                .forEach(t -> updaters.put(t, new DbCachedAddressOne(conn, t)));
    }

    @SuppressWarnings("DoubleCheckedLocking")
    public int getOrAdd(SrcAddress address) {
        checkArgument(address != null, "Argument 'address' is null");
        return getOne(address.getType()).getOrAdd(address.getHash());
    }

    @NonNull
    public BtcAddress add(BtcAddress btcAddress) {
        return getOne(btcAddress.getType()).add(btcAddress);
    }

    @NonNull
    public Optional<BtcAddress> getAddress(int addressId) {
        return getOne(BtcAddress.getTypeFromId(addressId)).getAddress(addressId);
    }

    @NonNull
    public Optional<BtcAddress> getAddress(Address address) {
        return getOne(Utils.getBtcAddressType(address.getOutputScriptType())).getAddress(address.getHash());
    }

    @NonNull
    private DbCachedAddressOne getOne(SrcAddressType addrType) {
        checkArgument(addrType != null, "Argument 'address' is null");
        checkArgument(addrType.isReal(), "Bad address type: %s", addrType);
        return updaters.get(addrType);
    }

    @Override
    public void close() {
        updaters.values().forEach(DbCachedAddressOne::close);
    }
}
