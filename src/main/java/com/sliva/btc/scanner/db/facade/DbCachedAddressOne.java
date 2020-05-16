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
import com.sliva.btc.scanner.db.model.BinaryAddress;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.CacheDualKeyNullable;
import com.sliva.btc.scanner.util.LazyInitializer;
import static com.sliva.btc.scanner.util.Utils.optionalBuilder2o;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedAddressOne implements AutoCloseable {

    private static final int MAX_CACHE_SIZE = 600_000;
    private final SrcAddressType addressType;
    private final DbUpdateAddressOne updateAddress;
    private final DbQueryAddressOne queryAddress;
    private final LazyInitializer<AtomicInteger> lastAddressId;
    private final Object syncAdd = new Object();
    private final CacheData cacheData = new CacheData();

    public DbCachedAddressOne(DBConnectionSupplier conn, SrcAddressType addressType) {
        checkArgument(conn != null, "Argument 'conn' is null");
        checkArgument(addressType != null, "Argument 'addressType' is null");
        checkArgument(addressType.isReal(), "Argument 'addressType' is not a real type: %s", addressType);
        this.addressType = addressType;
        updateAddress = new DbUpdateAddressOne(conn, addressType, cacheData.updateCachedData);
        queryAddress = new DbQueryAddressOne(conn, addressType);
        lastAddressId = new LazyInitializer<>(() -> new AtomicInteger(queryAddress.getLastAddressId()));
    }

    public int getOrAdd(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        return getAddress(address).orElseGet(() -> add(BtcAddress.builder().type(addressType).address(address).build())).getAddressId();
    }

    @NonNull
    public BtcAddress add(BtcAddress btcAddress) {
        checkArgument(btcAddress != null, "Argument 'btcAddress' is null");
        checkArgument(btcAddress.getAddress() != null, "Argument 'btcAddress.address' is null");
        BtcAddress result = btcAddress;
        boolean addressExist;
        synchronized (syncAdd) {
            addressExist = getIfPresentInCache(btcAddress.getAddress()).isPresent();
            if (!addressExist) {
                if (result.getAddressId() == 0) {
                    result = result.toBuilder().addressId(lastAddressId.get().incrementAndGet()).build();
                }
                cacheData.cache.put(result);
            }
        }
        if (!addressExist) {
            updateAddress.add(result);
        }
        return result;
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcAddress> getAddress(int addressId) {
        return cacheData.cache.get1(addressId, () -> optionalBuilder2o(cacheData.updateCachedData.getAddMapId().get(addressId), () -> queryAddress.findByAddressId(addressId)));
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcAddress> getAddress(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        BinaryAddress binAddr = new BinaryAddress(address);
        return cacheData.cache.get2(binAddr, () -> optionalBuilder2o(cacheData.updateCachedData.getAddMap().get(binAddr), () -> queryAddress.findByAddress(address)));
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(int addressId) {
        return cacheData.cache.getIfPresent1(addressId);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(BinaryAddress address) {
        checkArgument(address != null, "Argument 'address' is null");
        return cacheData.cache.getIfPresent2(address);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        return getIfPresentInCache(new BinaryAddress(address));
    }

    public boolean isPresentInCache(int addressId) {
        return cacheData.cache.isPresent1(addressId);
    }

    public boolean isPresentInCache(BinaryAddress address) {
        checkArgument(address != null, "Argument 'address' is null");
        return cacheData.cache.isPresent2(address);
    }

    @Override
    public void close() {
        log.debug("DbCachedAddressOne-{}.close()", addressType);
        updateAddress.close();
        cacheData.cache.invalidateAll();
    }

    @Getter
    private static class CacheData {

        private final CacheDualKeyNullable<Integer, BinaryAddress, BtcAddress> cache = new CacheDualKeyNullable<>(
                b -> b.concurrencyLevel(Runtime.getRuntime().availableProcessors())
                        .maximumSize(MAX_CACHE_SIZE)
                        .recordStats(),
                BtcAddress::getAddressId, BtcAddress::getAddress);
        private final DbUpdateAddressOne.CacheData updateCachedData = new DbUpdateAddressOne.CacheData();
    }
}
