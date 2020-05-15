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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.db.model.TXID;
import com.sliva.btc.scanner.util.LazyInitializer;
import static com.sliva.btc.scanner.util.Utils.optionalBuilder2o;
import java.util.List;
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
public class DbCachedTransaction implements AutoCloseable {

    private static final int MAX_CACHE_SIZE = 500_000;
    private final DbUpdateTransaction updateTransaction;
    private final DbQueryTransaction queryTransaction;
    private final LazyInitializer<AtomicInteger> lastTransactionId;
    private final Object syncAdd = new Object();
    @Getter
    @NonNull
    private final CacheData cacheData;

    public DbCachedTransaction(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbCachedTransaction(DBConnectionSupplier conn, CacheData cacheData) {
        checkArgument(conn != null, "Argument 'conn' is null");
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        this.cacheData = cacheData;
        updateTransaction = new DbUpdateTransaction(conn, cacheData.updateCachedData);
        queryTransaction = new DbQueryTransaction(conn);
        lastTransactionId = new LazyInitializer<>(() -> new AtomicInteger(queryTransaction.getLastTransactionId().orElse(0)));
    }

    @NonNull
    public BtcTransaction add(BtcTransaction btcTransaction) {
        checkArgument(btcTransaction != null, "Argument 'btcTransaction' is null");
        checkState(updateTransaction.isActive(), "Instance has been closed");
        boolean txExist;
        BtcTransaction result = btcTransaction;
        synchronized (syncAdd) {
            txExist = getIfPresentInCache(btcTransaction.getTxid()).isPresent();
            if (!txExist) {
                if (result.getTransactionId() == 0) {
                    result = result.toBuilder().transactionId(getNextTransactionId()).build();
                }
                updateCache(result);
            }
        }
        if (!txExist) {
            updateTransaction.add(result);
        }
        return result;
    }

    public void delete(BtcTransaction tx) {
        checkArgument(tx != null, "Argument 'tx' is null");
        removeFromCache(tx);
        updateTransaction.delete(tx);
    }

    @NonNull
    @SneakyThrows(ExecutionException.class)
    public Optional<BtcTransaction> getTransaction(int transactionId) {
        return cacheData.cacheMapId.get(transactionId, () -> updateMapId(optionalBuilder2o(
                updateTransaction.getCacheData().getAddMapId().get(transactionId),
                () -> queryTransaction.findTransaction(transactionId))));
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
        return cacheData.cacheMap.get(ttxid, () -> updateMap(optionalBuilder2o(
                updateTransaction.getCacheData().getAddMap().get(ttxid),
                () -> queryTransaction.findTransaction(txid))));
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
        return cacheData.cacheMap.get(ttxid, () -> updateMap(optionalBuilder2o(
                updateTransaction.getCacheData().getAddMap().get(ttxid),
                () -> queryTransaction.findTransactionId(txid).map(id -> BtcTransaction.builder().transactionId(id).txid(ttxid.getData()).build()))));
    }

    @NonNull
    public List<BtcTransaction> getTransactionsInBlock(int blockHeight) {
        List<BtcTransaction> result = queryTransaction.getTransactionsInBlock(blockHeight);
        result.forEach(this::updateCache);
        return result;
    }

    @NonNull
    public Optional<BtcTransaction> getIfPresentInCache(int transactionId) {
        return Optional.ofNullable(cacheData.cacheMapId.getIfPresent(transactionId)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcTransaction> getIfPresentInCache(TXID txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        return Optional.ofNullable(cacheData.cacheMap.getIfPresent(txid)).filter(Optional::isPresent).map(Optional::get);
    }

    @NonNull
    public Optional<BtcTransaction> getIfPresentInCache(byte[] txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        return getIfPresentInCache(new TXID(txid));
    }

    @Override
    public void close() {
        log.debug("DbCachedTransaction.close()");
        updateTransaction.close();
        cacheData.reset();
    }

    private void updateCache(BtcTransaction btcTransaction) {
        checkArgument(btcTransaction != null, "Argument 'btcTransaction' is null");
        cacheData.cacheMap.put(btcTransaction.getTxid(), Optional.of(btcTransaction));
        cacheData.cacheMapId.put(btcTransaction.getTransactionId(), Optional.of(btcTransaction));
    }

    private void removeFromCache(BtcTransaction tx) {
        cacheData.cacheMap.invalidate(tx.getTxid());
        cacheData.cacheMapId.invalidate(tx.getTransactionId());
    }

    private Optional<BtcTransaction> updateMap(Optional<BtcTransaction> ot) {
        ot.ifPresent(t -> cacheData.cacheMap.put(t.getTxid(), ot));
        return ot;
    }

    private Optional<BtcTransaction> updateMapId(Optional<BtcTransaction> ot) {
        ot.ifPresent(t -> cacheData.cacheMapId.put(t.getTransactionId(), ot));
        return ot;
    }

    private int getNextTransactionId() {
        return lastTransactionId.get().incrementAndGet();
    }

    @Getter
    private static class CacheData {

        private boolean active = true;
        private final Cache<TXID, Optional<BtcTransaction>> cacheMap = CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .maximumSize(MAX_CACHE_SIZE)
                .recordStats()
                .removalListener((RemovalNotification<TXID, Optional<BtcTransaction>> notification)
                        -> notification.getValue().filter(a -> active).map(BtcTransaction::getTransactionId).ifPresent(this::remove)
                ).build();
        private final Cache<Integer, Optional<BtcTransaction>> cacheMapId = CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .maximumSize(MAX_CACHE_SIZE)
                .recordStats()
                .removalListener((RemovalNotification<Integer, Optional<BtcTransaction>> notification)
                        -> notification.getValue().filter(a -> active).map(BtcTransaction::getTxid).ifPresent(cacheMap::invalidate)
                ).build();
        private final DbUpdateTransaction.CacheData updateCachedData = new DbUpdateTransaction.CacheData();

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
