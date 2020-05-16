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
import com.google.common.cache.CacheBuilder;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.db.model.TxOutput.TxOutputBuilder;
import com.sliva.btc.scanner.util.CacheNullableWrapper;
import static com.sliva.btc.scanner.util.Utils.optionalBuilder2o;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedOutput implements AutoCloseable {

    private static final int MAX_CACHE_SIZE = 1_000_000;
    private final CacheData cacheData;
    private final DbQueryOutput queryOutput;
    private final DbUpdateOutput updateOutput;

    public DbCachedOutput(DBConnectionSupplier conn) {
        checkArgument(conn != null, "Argument 'conn' is null");
        this.cacheData = new CacheData();
        this.queryOutput = new DbQueryOutput(conn);
        this.updateOutput = new DbUpdateOutput(conn, cacheData.updateCachedData);
    }

    public void add(TxOutput txOutput) {
        checkArgument(txOutput != null, "Argument 'txOutput' is null");
        cacheData.cacheMap.put(txOutput, Optional.of(txOutput));
        updateOutput.add(txOutput);
    }

    public void delete(TxOutput txOutput) {
        checkArgument(txOutput != null, "Argument 'txOutput' is null");
        updateOutput.delete(txOutput);
        cacheData.cacheMap.invalidate(txOutput);
    }

    public void updateStatus(int transactionId, short pos, byte status) {
        updateCacheValue(new InOutKey(transactionId, pos), b -> b.status(status));
        updateOutput.updateSpent(transactionId, pos, status);
    }

    public void updateAddress(int transactionId, short pos, int addressId) {
        updateCacheValue(new InOutKey(transactionId, pos), b -> b.addressId(addressId));
        updateOutput.updateAddress(transactionId, pos, addressId);
    }

    public void updateAmount(int transactionId, short pos, long amount) {
        updateCacheValue(new InOutKey(transactionId, pos), b -> b.amount(amount));
        updateOutput.updateAmount(transactionId, pos, amount);
    }

    @NonNull
    public Optional<TxOutput> getOutput(int transactionId, short pos) {
        return getOutput(new InOutKey(transactionId, pos));
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<TxOutput> getOutput(InOutKey key) {
        checkArgument(key != null, "Argument 'key' is null");
        return cacheData.cacheMap.get(key, () -> optionalBuilder2o(cacheData.updateCachedData.getQueueMap().get(key), () -> queryOutput.getOutput(key)));
    }

    @NonNull
    public Optional<TxOutput> getIfPresentInCache(InOutKey key) {
        checkArgument(key != null, "Argument 'key' is null");
        return cacheData.cacheMap.getIfPresent(key);
    }

    public boolean isPresentInCache(InOutKey key) {
        checkArgument(key != null, "Argument 'key' is null");
        return cacheData.cacheMap.isPresent(key);
    }

    @Override
    public void close() {
        log.debug("DbCachedOutput.close()");
        updateOutput.close();
        cacheData.cacheMap.invalidateAll();
    }

    private void updateCacheValue(InOutKey key, Function<TxOutputBuilder<?, ?>, TxOutputBuilder<?, ?>> updater) {
        getIfPresentInCache(key).ifPresent(txOutput -> cacheData.cacheMap.put(txOutput, Optional.of(updater.apply(txOutput.toBuilder()).build())));
    }

    @Getter
    private static class CacheData {

        private final CacheNullableWrapper<InOutKey, TxOutput> cacheMap = new CacheNullableWrapper<>(CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .maximumSize(MAX_CACHE_SIZE)
                .recordStats()
                .build());
        private final DbUpdateOutput.CacheData updateCachedData = new DbUpdateOutput.CacheData();
    }
}
