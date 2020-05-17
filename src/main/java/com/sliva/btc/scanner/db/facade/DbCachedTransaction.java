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
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.db.model.TXID;
import com.sliva.btc.scanner.util.CacheDualKeyNullable;
import com.sliva.btc.scanner.util.CommandLineUtils;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.LazyInitializer;
import static com.sliva.btc.scanner.util.LogUtils.printCacheStats;
import com.sliva.btc.scanner.util.TimerTaskWrapper;
import static com.sliva.btc.scanner.util.Utils.optionalBuilder2o;
import java.util.List;
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
public class DbCachedTransaction implements AutoCloseable {

    private static final int DEFAULT_MAX_CACHE_SIZE = 300_000;
    public static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions();
    public static final CommandLineUtils.CmdOption transactionCacheSizeOpt = buildOption(CMD_OPTS, null, "transaction-cache-size", true, "Transactions cache size. Default: " + DEFAULT_MAX_CACHE_SIZE);
    public static final CommandLineUtils.CmdOption printCacheStatsOpt = buildOption(CMD_OPTS, null, "print-cache-stats", true, "Print cache stats period in seconds. Default: 0 (off)");
    private static int transactionCacheSize = DEFAULT_MAX_CACHE_SIZE;
    private static int printCacheStatsPeriodSec;
    public static boolean CACHE_BY_ID_ENABLED = true;
    public static boolean CACHE_BY_TXID_ENABLED = true;

    private final DbUpdateTransaction updateTransaction;
    private final DbQueryTransaction queryTransaction;
    private final LazyInitializer<AtomicInteger> lastTransactionId;
    private final Object syncAdd = new Object();
    private final CacheDualKeyNullable<Integer, TXID, BtcTransaction> cache = new CacheDualKeyNullable<>(
            CACHE_BY_ID_ENABLED, CACHE_BY_TXID_ENABLED,
            b -> b.concurrencyLevel(Runtime.getRuntime().availableProcessors()).maximumSize(transactionCacheSize).recordStats(),
            BtcTransaction::getTransactionId, BtcTransaction::getTxid);
    private final Timer timer = new Timer();

    public static void applyArguments(CommandLineUtils.CmdArguments cmdArguments) {
        transactionCacheSize = cmdArguments.getOption(transactionCacheSizeOpt).map(Integer::valueOf).orElse(DEFAULT_MAX_CACHE_SIZE);
        printCacheStatsPeriodSec = cmdArguments.getOption(printCacheStatsOpt).map(Integer::valueOf).orElse(0);
    }

    public DbCachedTransaction(DBConnectionSupplier conn) {
        checkArgument(conn != null, "Argument 'conn' is null");
        updateTransaction = new DbUpdateTransaction(conn);
        queryTransaction = new DbQueryTransaction(conn);
        lastTransactionId = new LazyInitializer<>(() -> new AtomicInteger(queryTransaction.getLastTransactionId().orElse(0)));
        if (printCacheStatsPeriodSec > 0) {
            long msec = TimeUnit.SECONDS.toMillis(printCacheStatsPeriodSec);
            if (CACHE_BY_ID_ENABLED) {
                timer.scheduleAtFixedRate(new TimerTaskWrapper(() -> printCacheStats("transactions-1", cache.getStats1())), msec, msec);
            }
            if (CACHE_BY_TXID_ENABLED) {
                timer.scheduleAtFixedRate(new TimerTaskWrapper(() -> printCacheStats("transactions-2", cache.getStats2())), msec, msec);
            }
        }
    }

    @NonNull
    public BtcTransaction add(BtcTransaction btcTransaction) {
        checkArgument(btcTransaction != null, "Argument 'btcTransaction' is null");
        checkState(updateTransaction.isActive(), "Instance has been closed");
        boolean txExist;
        BtcTransaction result = btcTransaction;
        synchronized (syncAdd) {
            txExist = getIfPresentInCache(result.getTxid()).isPresent();
            if (!txExist) {
                if (result.getTransactionId() == 0) {
                    result = result.toBuilder().transactionId(lastTransactionId.get().incrementAndGet()).build();
                }
                cache.put(result);
            }
        }
        if (!txExist) {
            updateTransaction.add(result);
        }
        return result;
    }

    public void delete(BtcTransaction tx) {
        checkArgument(tx != null, "Argument 'tx' is null");
        checkState(updateTransaction.isActive(), "Instance has been closed");
        cache.invalidate(tx);
        updateTransaction.delete(tx);
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcTransaction> getTransaction(int transactionId) {
        return cache.get1(transactionId, this::_getTransactionNoCache);
    }

    /**
     * Retrieve BtcTransaction object from cache or DB by txid value.
     *
     * @param txid TXID value
     * @return Optional of BtcTransaction object
     */
    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcTransaction> getTransaction(String txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        TXID ttxid = TXID.build(txid);
        return cache.get2(ttxid, this::_getTransactionNoCache);
    }

    /**
     * Retrieve BtcTransaction object from cache or DB by txid value. The
     * difference from getTransaction(txid) is that this method returns
     * BtcTransaction with only transaction_id and txid set.
     *
     * @param txid TXID value
     * @return Optional of BtcTransaction object
     */
    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcTransaction> getTransactionSimple(String txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        TXID ttxid = TXID.build(txid);
        return cache.get2(ttxid, this::_getTransactionSimpleNoCache);
    }

    @NonNull
    public List<BtcTransaction> getTransactionsInBlock(int blockHeight) {
        List<BtcTransaction> result = queryTransaction.getTransactionsInBlock(blockHeight);
        result.forEach(cache::put);
        return result;
    }

    @NonNull
    public Optional<BtcTransaction> getIfPresentInCache(int transactionId) {
        return Optional.ofNullable(cache.getIfPresent1(transactionId)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcTransaction> getIfPresentInCache(TXID txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        return Optional.ofNullable(cache.getIfPresent2(txid)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcTransaction> getIfPresentInCache(byte[] txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        return getIfPresentInCache(new TXID(txid));
    }

    public boolean isPresentInCache(int transactionId) {
        return cache.isPresent1(transactionId);
    }

    public boolean isPresentInCache(TXID txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        return cache.isPresent2(txid);
    }

    @Override
    public void close() {
        log.debug("DbCachedTransaction.close()");
        updateTransaction.close();
        cache.invalidateAll();
        timer.cancel();
    }

    @NonNull
    private Optional<BtcTransaction> _getTransactionNoCache(int transactionId) {
        return optionalBuilder2o(
                updateTransaction.getFromCache(transactionId),
                transactionId, queryTransaction::findTransaction);
    }

    @NonNull
    private Optional<BtcTransaction> _getTransactionNoCache(TXID txid) {
        return optionalBuilder2o(
                updateTransaction.getFromCache(txid),
                txid, queryTransaction::findTransaction);
    }

    @NonNull
    private Optional<BtcTransaction> _getTransactionSimpleNoCache(TXID txid) {
        return optionalBuilder2o(
                updateTransaction.getFromCache(txid),
                txid, this::_loadTransactionSimple);
    }

    @NonNull
    private Optional<BtcTransaction> _loadTransactionSimple(TXID txid) {
        return queryTransaction.findTransactionId(txid).map(id -> BtcTransaction.builder().transactionId(id).txid(txid.getData()).build());
    }
}
