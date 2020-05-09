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

import static com.sliva.btc.scanner.db.DbUpdate.waitFullQueue;
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxInput;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateInput extends DbUpdate {

    public static int MIN_BATCH_SIZE = 1;
    public static int MAX_BATCH_SIZE = 40000;
    public static int MAX_INSERT_QUEUE_LENGTH = 1000000;
    private static int MAX_UPDATE_QUEUE_LENGTH = 2000;
    private static final String TABLE_NAME = "input";
    private static final String SQL_ADD = "INSERT INTO input(transaction_id,pos,in_transaction_id,in_pos)VALUES(?,?,?,?)";
    private static final String SQL_DELETE = "DELETE FROM input WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE = "UPDATE input SET in_transaction_id=?,in_pos=? WHERE transaction_id=? AND pos=?";
    private final DBPreparedStatement psAdd;
    private final DBPreparedStatement psDelete;
    private final DBPreparedStatement psUpdate;
    private final CacheData cacheData;

    public DbUpdateInput(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbUpdateInput(DBConnectionSupplier conn, CacheData cacheData) {
        super(conn);
        this.psAdd = conn.prepareStatement(SQL_ADD);
        this.psDelete = conn.prepareStatement(SQL_DELETE);
        this.psUpdate = conn.prepareStatement(SQL_UPDATE);
        this.cacheData = cacheData;
    }

    public CacheData getCacheData() {
        return cacheData;
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public int getCacheFillPercent() {
        return cacheData == null ? 0 : Math.max(cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH, cacheData.queueUpdate.size() * 100 / MAX_UPDATE_QUEUE_LENGTH);
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData != null && (cacheData.addQueue.size() >= MIN_BATCH_SIZE || cacheData.queueUpdate.size() >= MIN_BATCH_SIZE);
    }

    public void add(TxInput txInput) throws SQLException {
        log.trace("add(txInput:{})", txInput);
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        synchronized (cacheData) {
            cacheData.addQueue.add(txInput);
            cacheData.queueMap.put(txInput, txInput);
            List<TxInput> list = cacheData.queueMapTx.get(txInput.getTransactionId());
            if (list == null) {
                cacheData.queueMapTx.put(txInput.getTransactionId(), list = new ArrayList<>());
            }
            list.add(txInput);
        }
    }

    public void delete(TxInput txInput) throws SQLException {
        log.trace("delete(txInput:{})", txInput);
        synchronized (cacheData) {
            psDelete.setParameters(p -> p.setInt(txInput.getTransactionId()).setInt(txInput.getPos())).execute();
            cacheData.addQueue.remove(txInput);
            cacheData.queueMap.remove(txInput);
            List<TxInput> l = cacheData.queueMapTx.get(txInput.getTransactionId());
            if (l != null) {
                l.remove(txInput);
                if (l.isEmpty()) {
                    cacheData.queueMapTx.remove(txInput.getTransactionId());
                }
            }
        }
    }

    public void update(TxInput txInput) throws SQLException {
        log.trace("update(txInput:{})", txInput);
        synchronized (cacheData) {
            TxInput txInput2 = cacheData.queueMap.get(txInput);
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
                (t, p) -> p.setInt(t.getTransactionId()).setInt(t.getPos()).setInt(t.getInTransactionId()).setInt(t.getInPos()),
                executed -> {
                    synchronized (cacheData) {
                        executed.stream().peek(cacheData.queueMap::remove).map(InOutKey::getTransactionId).forEach(cacheData.queueMapTx::remove);
                    }
                });
    }

    @Override
    public int executeUpdates() {
        return executeBatch(cacheData, cacheData.queueUpdate, psUpdate, MAX_BATCH_SIZE,
                (t, p) -> p.setInt(t.getInTransactionId()).setInt(t.getInPos()).setInt(t.getTransactionId()).setInt(t.getPos()), null);
    }

    @Getter
    public static class CacheData {

        private final Collection<TxInput> addQueue = new LinkedHashSet<>();
        private final Map<InOutKey, TxInput> queueMap = new HashMap<>();
        private final Map<Integer, List<TxInput>> queueMapTx = new HashMap<>();
        private final Collection<TxInput> queueUpdate = new ArrayList<>();
    }
}
