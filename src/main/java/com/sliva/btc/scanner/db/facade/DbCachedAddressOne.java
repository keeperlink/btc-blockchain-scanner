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
import static com.sliva.btc.scanner.db.facade.DbQueryAddressOne.getAddressTableName;
import com.sliva.btc.scanner.db.model.BinaryAddress;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.CacheDualKeyNullable;
import com.sliva.btc.scanner.util.CommandLineUtils;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.LazyInitializer;
import static com.sliva.btc.scanner.util.LogUtils.printCacheStats;
import com.sliva.btc.scanner.util.TimerTaskWrapper;
import static com.sliva.btc.scanner.util.Utils.optionalBuilder2o;
import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedAddressOne implements AutoCloseable {

    private static final int DEFAULT_MAX_CACHE_SIZE = 300_000;
    public static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions();
    public static final CommandLineUtils.CmdOption addressCacheSizeOpt = buildOption(CMD_OPTS, null, "address-cache-size", true, "Addresses cache size. Default: " + DEFAULT_MAX_CACHE_SIZE);
    public static final CommandLineUtils.CmdOption printCacheStatsOpt = buildOption(CMD_OPTS, null, "print-cache-stats", true, "Print cache stats period in seconds. Default: 0 (off)");
    private static int addressCacheSize = DEFAULT_MAX_CACHE_SIZE;
    private static int printCacheStatsPeriodSec;
    public static boolean CACHE_BY_ID_ENABLED = true;
    public static boolean CACHE_BY_NAME_ENABLED = true;

    private final SrcAddressType addressType;
    private final DbUpdateAddressOne updateAddress;
    private final DbQueryAddressOne queryAddress;
    private final LazyInitializer<AtomicInteger> lastAddressId;
    private final Object syncAdd = new Object();
    private final CacheDualKeyNullable<Integer, BinaryAddress, BtcAddress> cache = new CacheDualKeyNullable<>(
            CACHE_BY_ID_ENABLED, CACHE_BY_NAME_ENABLED,
            b -> b.concurrencyLevel(Runtime.getRuntime().availableProcessors()).maximumSize(addressCacheSize).recordStats(),
            BtcAddress::getAddressId, BtcAddress::getAddress);
    private final Timer timer = new Timer();

    public static void applyArguments(CommandLineUtils.CmdArguments cmdArguments) {
        addressCacheSize = cmdArguments.getOption(addressCacheSizeOpt).map(Integer::valueOf).orElse(DEFAULT_MAX_CACHE_SIZE);
        printCacheStatsPeriodSec = cmdArguments.getOption(printCacheStatsOpt).map(Integer::valueOf).orElse(0);
    }

    public DbCachedAddressOne(DBConnectionSupplier conn, SrcAddressType addressType) {
        checkArgument(conn != null, "Argument 'conn' is null");
        checkArgument(addressType != null, "Argument 'addressType' is null");
        checkArgument(addressType.isReal(), "Argument 'addressType' is not a real type: %s", addressType);
        this.addressType = addressType;
        updateAddress = new DbUpdateAddressOne(conn, addressType);
        queryAddress = new DbQueryAddressOne(conn, addressType);
        lastAddressId = new LazyInitializer<>(() -> new AtomicInteger(queryAddress.getLastAddressId()));
        if (printCacheStatsPeriodSec > 0) {
            long msec = TimeUnit.SECONDS.toMillis(printCacheStatsPeriodSec);
            if (CACHE_BY_ID_ENABLED) {
                timer.scheduleAtFixedRate(new TimerTaskWrapper(() -> printCacheStats(getAddressTableName(addressType) + "-1", cache.getStats1())), msec, msec);
            }
            if (CACHE_BY_NAME_ENABLED) {
                timer.scheduleAtFixedRate(new TimerTaskWrapper(() -> printCacheStats(getAddressTableName(addressType) + "-2", cache.getStats2())), msec, msec);
            }
        }
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
            addressExist = getIfPresentInCache(result.getAddress()).isPresent();
            if (!addressExist) {
                if (result.getAddressId() == 0) {
                    result = result.toBuilder().addressId(lastAddressId.get().incrementAndGet()).build();
                }
                cache.put(result);
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
        return cache.get1(addressId, this::_getAddressNoCache);
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcAddress> getAddress(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        BinaryAddress binAddr = new BinaryAddress(address);
        return cache.get2(binAddr, this::_getAddressNoCache);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(int addressId) {
        return cache.getIfPresent1(addressId);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(BinaryAddress address) {
        checkArgument(address != null, "Argument 'address' is null");
        return cache.getIfPresent2(address);
    }

    @NonNull
    public Optional<BtcAddress> getIfPresentInCache(byte[] address) {
        checkArgument(address != null, "Argument 'address' is null");
        return getIfPresentInCache(new BinaryAddress(address));
    }

    public boolean isPresentInCache(int addressId) {
        return cache.isPresent1(addressId);
    }

    public boolean isPresentInCache(BinaryAddress address) {
        checkArgument(address != null, "Argument 'address' is null");
        return cache.isPresent2(address);
    }

    @Override
    public void close() {
        log.debug("DbCachedAddressOne-{}.close()", addressType);
        updateAddress.close();
        cache.invalidateAll();
        timer.cancel();
    }

    private Optional<BtcAddress> _getAddressNoCache(int id) {
        return optionalBuilder2o(updateAddress.getCacheData().getAddMapId().get(id), id, queryAddress::findByAddressId);
    }

    private Optional<BtcAddress> _getAddressNoCache(BinaryAddress a) {
        return optionalBuilder2o(updateAddress.getCacheData().getAddMap().get(a), a, queryAddress::findByAddress);
    }
}
