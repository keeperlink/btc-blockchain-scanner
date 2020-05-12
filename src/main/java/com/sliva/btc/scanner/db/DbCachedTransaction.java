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

import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import static com.sliva.btc.scanner.util.Utils.optionalBuilder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbCachedTransaction implements AutoCloseable {

    private static final int MAX_CACHE_SIZE = 50000;
    private final DbUpdateTransaction updateTransaction;
    private final DbQueryTransaction queryTransaction;
    @Getter
    @NonNull
    private final CacheData cacheData;

    public DbCachedTransaction(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbCachedTransaction(DBConnectionSupplier conn, CacheData cacheData) {
        checkArgument(conn != null, "Argument 'conn' is null");
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        updateTransaction = new DbUpdateTransaction(conn, cacheData.updateCachedData);
        queryTransaction = new DbQueryTransaction(conn);
        this.cacheData = cacheData;
    }

    @NonNull
    public BtcTransaction add(BtcTransaction btcTransaction) {
        checkArgument(btcTransaction != null, "Argument 'btcTransaction' is null");
        synchronized (cacheData) {
            BtcTransaction result = btcTransaction;
            if (result.getTransactionId() == 0) {
                if (cacheData.lastTransactionId.get() == 0) {
                    cacheData.lastTransactionId.set(queryTransaction.getLastTransactionId().orElse(0));
                }
                result = result.toBuilder().transactionId(cacheData.lastTransactionId.incrementAndGet()).build();
            }
            updateTransaction.add(result);
            updateCache(result);
            return result;
        }
    }

    public void delete(BtcTransaction tx) {
        checkArgument(tx != null, "Argument 'tx' is null");
        synchronized (cacheData) {
            updateTransaction.delete(tx);
            cacheData.cacheMap.remove(tx.getTxid());
            cacheData.cacheMapId.remove(tx.getTransactionId());
        }
    }

    @NonNull
    public Optional<BtcTransaction> getTransaction(int transactionId) {
        Optional<BtcTransaction> result = Optional.ofNullable(cacheData.cacheMapId.get(transactionId));
        if (!result.isPresent()) {
            result = Optional.ofNullable(updateTransaction.getCacheData().getAddMapId().get(transactionId));
        }
        if (!result.isPresent()) {
            result = queryTransaction.findTransaction(transactionId);
        }
        result.ifPresent(this::updateCache);
        return result;
    }

    /**
     * Retrieve BtcTransaction object from cache or DB by txid value.
     *
     * @param txid TXID value
     * @return Optional of BtcTransaction object
     */
    @NonNull
    public Optional<BtcTransaction> getTransaction(String txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        Optional<BtcTransaction> result = optionalBuilder(
                cacheData.cacheMap.get(txid),
                () -> updateTransaction.getCacheData().getAddMap().get(txid),
                () -> queryTransaction.findTransaction(txid));
        result.ifPresent(this::updateCache);
        return result;
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
    public Optional<BtcTransaction> getTransactionSimple(String txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        Optional<BtcTransaction> result = optionalBuilder(
                cacheData.cacheMap.get(txid),
                () -> updateTransaction.getCacheData().getAddMap().get(txid),
                () -> queryTransaction.findTransactionId(txid).map(id -> BtcTransaction.builder().transactionId(id).txid(txid).build()));
        result.ifPresent(this::updateCache);
        return result;
    }

    @NonNull
    public List<BtcTransaction> getTransactionsInBlock(int blockHeight) {
        List<BtcTransaction> result = queryTransaction.getTransactionsInBlock(blockHeight);
        result.forEach(this::updateCache);
        return result;
    }

    private void updateCache(BtcTransaction btcTransaction) {
        checkArgument(btcTransaction != null, "Argument 'btcTransaction' is null");
        synchronized (cacheData) {
            cacheData.cacheMap.remove(btcTransaction.getTxid());
            cacheData.cacheMap.put(btcTransaction.getTxid(), btcTransaction);
            cacheData.cacheMapId.put(btcTransaction.getTransactionId(), btcTransaction);
            if (cacheData.cacheMap.size() >= MAX_CACHE_SIZE) {
                BtcTransaction t = cacheData.cacheMap.entrySet().iterator().next().getValue();
                cacheData.cacheMap.remove(t.getTxid());
                cacheData.cacheMapId.remove(t.getTransactionId());
            }
        }
    }

    @Override
    public void close() {
        log.debug("DbCachedTransaction.close()");
        synchronized (cacheData) {
            updateTransaction.close();
        }
    }

    @Getter
    public static class CacheData {

        private final Map<String, BtcTransaction> cacheMap = new LinkedHashMap<>();
        private final Map<Integer, BtcTransaction> cacheMapId = new HashMap<>();
        private final AtomicInteger lastTransactionId = new AtomicInteger(0);
        private final DbUpdateTransaction.CacheData updateCachedData = new DbUpdateTransaction.CacheData();
    }
}
