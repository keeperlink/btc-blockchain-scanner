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
import static com.google.common.base.Preconditions.checkState;
import static com.sliva.btc.scanner.db.DbUpdate.waitFullQueue;
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxInputSpecial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateInputSpecial extends DbUpdate {

    private static int MIN_BATCH_SIZE = 1;
    private static int MAX_BATCH_SIZE = 40000;
    private static int MAX_INSERT_QUEUE_LENGTH = 10000;
    private static int MAX_UPDATE_QUEUE_LENGTH = 10000;
    private static final String TABLE_NAME = "input";
    private static final String SQL_ADD = "INSERT INTO input_special(transaction_id,pos,sighash_type,segwit,multisig)VALUES(?,?,?,?,?)";
    private static final String SQL_DELETE = "DELETE FROM input_special WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE = "UPDATE input_special SET sighash_type=?,segwit=?,multisig=? WHERE transaction_id=? AND pos=?";
    private final DBPreparedStatement psAdd;
    private final DBPreparedStatement psDelete;
    private final DBPreparedStatement psUpdate;
    @Getter
    @NonNull
    private final CacheData cacheData;

    public DbUpdateInputSpecial(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbUpdateInputSpecial(DBConnectionSupplier conn, CacheData cacheData) {
        super(TABLE_NAME, conn);
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        this.psAdd = conn.prepareStatement(SQL_ADD);
        this.psDelete = conn.prepareStatement(SQL_DELETE);
        this.psUpdate = conn.prepareStatement(SQL_UPDATE);
        this.cacheData = cacheData;
    }

    @Override
    public int getCacheFillPercent() {
        return Math.max(cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH, cacheData.queueUpdate.size() * 100 / MAX_UPDATE_QUEUE_LENGTH);
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData.addQueue.size() >= MIN_BATCH_SIZE || cacheData.queueUpdate.size() >= MIN_BATCH_SIZE;
    }

    public void add(TxInputSpecial txInput) {
        log.trace("add(txInput:{})", txInput);
        checkState(isActive(), "Instance has been closed");
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        synchronized (cacheData) {
            cacheData.addQueue.add(txInput);
            cacheData.queueMap.put(txInput, txInput);
            List<TxInputSpecial> list = cacheData.queueMapTx.get(txInput.getTransactionId());
            if (list == null) {
                cacheData.queueMapTx.put(txInput.getTransactionId(), list = new ArrayList<>());
            }
            list.add(txInput);
        }
    }

    public void delete(TxInputSpecial txInput) {
        log.trace("delete(txInput:{})", txInput);
        checkState(isActive(), "Instance has been closed");
        synchronized (cacheData) {
            psDelete.setParameters(p -> p.setInt(txInput.getTransactionId()).setInt(txInput.getPos())).execute();
            cacheData.addQueue.remove(txInput);
            cacheData.queueMap.remove(txInput);
            List<TxInputSpecial> l = cacheData.queueMapTx.get(txInput.getTransactionId());
            if (l != null) {
                l.remove(txInput);
                if (l.isEmpty()) {
                    cacheData.queueMapTx.remove(txInput.getTransactionId());
                }
            }
        }
    }

    public void update(TxInputSpecial txInput) {
        log.trace("update(txInput:{})", txInput);
        checkState(isActive(), "Instance has been closed");
        synchronized (cacheData) {
            TxInputSpecial txInput2 = cacheData.queueMap.get(txInput);
            boolean updatedInQueue = false;
            if (txInput2 != null) {
                if (cacheData.addQueue.remove(txInput2)) {
                    cacheData.addQueue.add(txInput);
                    updatedInQueue = true;
                }
                cacheData.queueMap.put(txInput, txInput);
                //cacheData.queueMapTx.put(key, txInput);
            }
            if (!updatedInQueue) {
                cacheData.queueUpdate.add(txInput);
            }
        }
        if (cacheData.queueUpdate.size() >= MAX_UPDATE_QUEUE_LENGTH) {
            executeUpdates();
        }
    }

    @SuppressWarnings({"UseSpecificCatch"})
    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, MAX_BATCH_SIZE,
                (t, p) -> p.setInt(t.getTransactionId()).setInt(t.getPos()).setInt(Byte.toUnsignedInt(t.getSighashType())).setBoolean(t.isSegwit()).setBoolean(t.isMultisig()),
                executed -> {
                    synchronized (cacheData) {
                        executed.stream().peek(cacheData.queueMap::remove).map(InOutKey::getTransactionId).forEach(cacheData.queueMapTx::remove);
                    }
                });
    }

    @Override
    public int executeUpdates() {
        return executeBatch(cacheData, cacheData.queueUpdate, psUpdate, MAX_BATCH_SIZE,
                (t, p) -> p.setInt(Byte.toUnsignedInt(t.getSighashType())).setBoolean(t.isSegwit()).setBoolean(t.isMultisig()).setInt(t.getTransactionId()).setInt(t.getPos()), null);
    }

    @Getter
    public static class CacheData {

        private final Collection<TxInputSpecial> addQueue = new LinkedHashSet<>();
        private final Map<InOutKey, TxInputSpecial> queueMap = new HashMap<>();
        private final Map<Integer, List<TxInputSpecial>> queueMapTx = new HashMap<>();
        private final Collection<TxInputSpecial> queueUpdate = new ArrayList<>();
    }
}
