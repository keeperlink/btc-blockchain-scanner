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
import static com.google.common.base.Preconditions.checkState;
import com.google.common.cache.CacheBuilder;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.db.model.TxOutput.TxOutputBuilder;
import com.sliva.btc.scanner.util.CacheNullableWrapper;
import com.sliva.btc.scanner.util.CommandLineUtils;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import static com.sliva.btc.scanner.util.LogUtils.printCacheStats;
import com.sliva.btc.scanner.util.TimerTaskWrapper;
import static com.sliva.btc.scanner.util.Utils.optionalBuilder2o;
import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedOutput implements AutoCloseable {

    private static final int DEFAULT_MAX_CACHE_SIZE = 300_000;
    public static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions();
    public static final CommandLineUtils.CmdOption outputCacheSizeOpt = buildOption(CMD_OPTS, null, "output-cache-size", true, "Outputs cache size. Default: " + DEFAULT_MAX_CACHE_SIZE);
    public static final CommandLineUtils.CmdOption printCacheStatsOpt = buildOption(CMD_OPTS, null, "print-cache-stats", true, "Print cache stats period in seconds. Default: 0 (off)");
    private static int outputCacheSize = DEFAULT_MAX_CACHE_SIZE;
    private static int printCacheStatsPeriodSec;

    private final DbQueryOutput queryOutput;
    private final DbUpdateOutput updateOutput;
    private final CacheNullableWrapper<InOutKey, TxOutput> cache = new CacheNullableWrapper<>(CacheBuilder.newBuilder()
            .concurrencyLevel(Runtime.getRuntime().availableProcessors())
            .maximumSize(outputCacheSize).recordStats().build());
    private final Timer timer = new Timer();

    public static void applyArguments(CommandLineUtils.CmdArguments cmdArguments) {
        outputCacheSize = cmdArguments.getOption(outputCacheSizeOpt).map(Integer::valueOf).orElse(DEFAULT_MAX_CACHE_SIZE);
        printCacheStatsPeriodSec = cmdArguments.getOption(printCacheStatsOpt).map(Integer::valueOf).orElse(0);
    }

    public DbCachedOutput(DBConnectionSupplier conn) {
        checkArgument(conn != null, "Argument 'conn' is null");
        this.queryOutput = new DbQueryOutput(conn);
        this.updateOutput = new DbUpdateOutput(conn);
        if (printCacheStatsPeriodSec > 0) {
            long msec = TimeUnit.SECONDS.toMillis(printCacheStatsPeriodSec);
            timer.scheduleAtFixedRate(new TimerTaskWrapper(() -> printCacheStats("output", cache.stats())), msec, msec);
        }
    }

    public void add(TxOutput txOutput) {
        checkArgument(txOutput != null, "Argument 'txOutput' is null");
        checkState(updateOutput.isActive(), "Instance has been closed");
        cache.put(txOutput, Optional.of(txOutput));
        updateOutput.add(txOutput);
    }

    public void delete(TxOutput txOutput) {
        checkArgument(txOutput != null, "Argument 'txOutput' is null");
        checkState(updateOutput.isActive(), "Instance has been closed");
        updateOutput.delete(txOutput);
        cache.invalidate(txOutput);
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
        return cache.get(key, () -> optionalBuilder2o(updateOutput.getCacheData().getQueueMap().get(key), key, queryOutput::getOutput));
    }

    @NonNull
    public Optional<TxOutput> getIfPresentInCache(InOutKey key) {
        checkArgument(key != null, "Argument 'key' is null");
        return cache.getIfPresent(key);
    }

    public boolean isPresentInCache(InOutKey key) {
        checkArgument(key != null, "Argument 'key' is null");
        return cache.isPresent(key);
    }

    @Override
    public void close() {
        log.debug("DbCachedOutput.close()");
        updateOutput.close();
        cache.invalidateAll();
        timer.cancel();
    }

    private void updateCacheValue(InOutKey key, Function<TxOutputBuilder<?, ?>, TxOutputBuilder<?, ?>> updater) {
        getIfPresentInCache(key).ifPresent(txOutput -> cache.put(txOutput, Optional.of(updater.apply(txOutput.toBuilder()).build())));
    }
}
