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

    private static final int MAX_CACHE_SIZE = 300000;
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
        updateAddress = new DbUpdateAddressOne(conn, addressType, cacheData.updateCachedData);
        queryAddress = new DbQueryAddressOne(conn, addressType);
        lastAddressId = new LazyInitializer<>(() -> new AtomicInteger(queryAddress.getLastAddressId()));
        this.cacheData = cacheData;
    }

    public int getOrAdd(byte[] address) {
        return getAddress(address).orElseGet(() -> add(BtcAddress.builder().type(addressType).addressId(getNextAddressId()).address(address).build())).getAddressId();
    }

    @NonNull
    public BtcAddress add(BtcAddress btcAddress) {
        BtcAddress a = btcAddress;
        boolean addressExist;
        synchronized (syncAdd) {
            addressExist = getIfPresentInCache(btcAddress.getAddress()).isPresent();
            if (!addressExist) {
                if (a.getAddressId() == 0) {
                    a = a.toBuilder().addressId(getNextAddressId()).build();
                }
                cacheData.cacheMap.put(a.getAddress(), Optional.of(a));
                cacheData.cacheMapId.put(a.getAddressId(), Optional.of(a));
            }
        }
        if (!addressExist) {
            updateAddress.add(a);
        }
        return a;
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcAddress> getAddress(int addressId) {
        return cacheData.cacheMapId.get(addressId, () -> optionalBuilder2o(cacheData.updateCachedData.getAddMapId().get(addressId), () -> updateMap(queryAddress.findByAddressId(addressId))));
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcAddress> getAddress(byte[] address) {
        BinaryAddress binAddr = new BinaryAddress(address);
        return cacheData.cacheMap.get(binAddr, () -> optionalBuilder2o(cacheData.updateCachedData.getAddMap().get(binAddr), () -> updateMapId(queryAddress.findByAddress(address))));
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(int addressId) {
        return Optional.ofNullable(cacheData.cacheMapId.getIfPresent(addressId)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(BinaryAddress address) {
        return Optional.ofNullable(cacheData.cacheMap.getIfPresent(address)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(byte[] address) {
        return getIfPresentInCache(new BinaryAddress(address));
    }

    private int getNextAddressId() {
        return lastAddressId.get().incrementAndGet();
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
        synchronized (cacheData) {
            updateAddress.close();
        }
    }

    @Getter
    public static class CacheData {

        private final Cache<BinaryAddress, Optional<BtcAddress>> cacheMap;
        private final Cache<Integer, Optional<BtcAddress>> cacheMapId;

        private CacheData() {
            cacheMap = CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .maximumSize(MAX_CACHE_SIZE)
                    .removalListener((RemovalNotification<BinaryAddress, Optional<BtcAddress>> notification)
                            -> notification.getValue().map(BtcAddress::getAddressId).ifPresent(this::remove)
                    ).recordStats()
                    .build();
            cacheMapId = CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .maximumSize(MAX_CACHE_SIZE)
                    .removalListener((RemovalNotification<Integer, Optional<BtcAddress>> notification)
                            -> notification.getValue().map(BtcAddress::getAddress).ifPresent(this::remove)
                    ).recordStats()
                    .build();
        }

        private void remove(BinaryAddress key) {
            cacheMap.invalidate(key);
        }

        private void remove(Integer key) {
            cacheMapId.invalidate(key);
        }
        //private final Map<BinaryAddress, BtcAddress> cacheMap = new LinkedHashMap<>();
        private final DbUpdateAddressOne.CacheData updateCachedData = new DbUpdateAddressOne.CacheData();
    }
}
