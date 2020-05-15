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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.model.BinaryAddress;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
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
    @Getter
    @NonNull
    private final CacheData cacheData;

    public DbCachedAddressOne(DBConnectionSupplier conn, SrcAddressType addressType) {
        this(conn, addressType, new CacheData());
    }

    public DbCachedAddressOne(DBConnectionSupplier conn, SrcAddressType addressType, CacheData cacheData) {
        checkArgument(addressType != null, "Argument 'addressType' is null");
        checkArgument(addressType.isReal(), "Argument 'addressType' is not a real type: %s", addressType);
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        this.addressType = addressType;
        this.cacheData = cacheData;
        updateAddress = new DbUpdateAddressOne(conn, addressType, cacheData.updateCachedData);
        queryAddress = new DbQueryAddressOne(conn, addressType);
        lastAddressId = new LazyInitializer<>(() -> new AtomicInteger(queryAddress.getLastAddressId()));
    }

    public int getOrAdd(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        return getAddress(address).orElseGet(() -> add(BtcAddress.builder().type(addressType).addressId(getNextAddressId()).address(address).build())).getAddressId();
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
                    result = result.toBuilder().addressId(getNextAddressId()).build();
                }
                updateCache(result);
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
        return cacheData.cacheMapId.get(addressId, () -> updateMap(optionalBuilder2o(cacheData.updateCachedData.getAddMapId().get(addressId), () -> queryAddress.findByAddressId(addressId))));
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcAddress> getAddress(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        BinaryAddress binAddr = new BinaryAddress(address);
        return cacheData.cacheMap.get(binAddr, () -> updateMapId(optionalBuilder2o(cacheData.updateCachedData.getAddMap().get(binAddr), () -> queryAddress.findByAddress(address))));
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(int addressId) {
        return Optional.ofNullable(cacheData.cacheMapId.getIfPresent(addressId)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(BinaryAddress address) {
        checkArgument(address != null, "Argument 'address' is null");
        return Optional.ofNullable(cacheData.cacheMap.getIfPresent(address)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        return getIfPresentInCache(new BinaryAddress(address));
    }

    private int getNextAddressId() {
        return lastAddressId.get().incrementAndGet();
    }

    private void updateCache(BtcAddress address) {
        cacheData.cacheMap.put(address.getAddress(), Optional.of(address));
        cacheData.cacheMapId.put(address.getAddressId(), Optional.of(address));
    }

    private Optional<BtcAddress> updateMap(Optional<BtcAddress> oa) {
        oa.ifPresent(a -> cacheData.cacheMap.put(a.getAddress(), oa));
        return oa;
    }

    private Optional<BtcAddress> updateMapId(Optional<BtcAddress> oa) {
        oa.ifPresent(a -> cacheData.cacheMapId.put(a.getAddressId(), oa));
        return oa;
    }

    @Override
    public void close() {
        log.debug("DbCachedAddressOne-{}.close()", addressType);
        updateAddress.close();
        cacheData.reset();
    }

    @Getter
    private static class CacheData {

        private boolean active = true;
        private final Cache<BinaryAddress, Optional<BtcAddress>> cacheMap = CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .maximumSize(MAX_CACHE_SIZE)
                .removalListener((RemovalNotification<BinaryAddress, Optional<BtcAddress>> notification)
                        -> notification.getValue().filter(a -> active).map(BtcAddress::getAddressId).ifPresent(this::remove)
                ).recordStats()
                .build();
        private final Cache<Integer, Optional<BtcAddress>> cacheMapId = CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .maximumSize(MAX_CACHE_SIZE)
                .removalListener((RemovalNotification<Integer, Optional<BtcAddress>> notification)
                        -> notification.getValue().filter(a -> active).map(BtcAddress::getAddress).ifPresent(cacheMap::invalidate)
                ).recordStats()
                .build();
        private final DbUpdateAddressOne.CacheData updateCachedData = new DbUpdateAddressOne.CacheData();

        private void remove(Integer key) {
            cacheMapId.invalidate(key);
        }

        private void reset() {
            active = false;
            cacheMap.invalidateAll();
            cacheMapId.invalidateAll();
        }
    }
}
