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
import com.sliva.btc.scanner.db.model.TxInputSpecial;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
public class DbUpdateInputSpecial extends DbUpdate {

    public static int MIN_BATCH_SIZE = 1;
    public static int MAX_BATCH_SIZE = 40000;
    public static int MAX_INSERT_QUEUE_LENGTH = 1000000;
    private static int MAX_UPDATE_QUEUE_LENGTH = 100;
    private static final String TABLE_NAME = "input";
    private static final String SQL_ADD = "INSERT INTO input_special(transaction_id,pos,sighash_type,segwit,multisig)VALUES(?,?,?,?,?)";
    private static final String SQL_DELETE = "DELETE FROM input_special WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE = "UPDATE input_special SET sighash_type=?,segwit=?,multisig=? WHERE transaction_id=? AND pos=?";
    private final ThreadLocal<PreparedStatement> psAdd;
    private final ThreadLocal<PreparedStatement> psDelete;
    private final ThreadLocal<PreparedStatement> psUpdate;
    private final CacheData cacheData;

    public DbUpdateInputSpecial(DBConnection conn) {
        this(conn, new CacheData());
    }

    public DbUpdateInputSpecial(DBConnection conn, CacheData cacheData) {
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
        return cacheData == null ? 0 : cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH;
    }

    @Override
    public boolean needExecuteInserts() {
        return cacheData == null ? false : cacheData.addQueue.size() >= MIN_BATCH_SIZE;
    }

    public void add(TxInputSpecial txInput) throws SQLException {
        log.trace("add(txInput:{})", txInput);
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        synchronized (cacheData) {
            cacheData.addQueue.add(txInput);
            cacheData.queueMap.put(new InOutKey(txInput.getTransactionId(), txInput.getPos()), txInput);
            List<TxInputSpecial> list = cacheData.queueMapTx.get(txInput.getTransactionId());
            if (list == null) {
                cacheData.queueMapTx.put(txInput.getTransactionId(), list = new ArrayList<>());
            }
            list.add(txInput);
        }
    }

    public void delete(TxInputSpecial txInput) throws SQLException {
        log.trace("delete(txInput:{})", txInput);
        synchronized (cacheData) {
            psDelete.get().setInt(1, txInput.getTransactionId());
            psDelete.get().setInt(2, txInput.getPos());
            psDelete.get().execute();
            cacheData.addQueue.remove(txInput);
            cacheData.queueMap.remove(new InOutKey(txInput.getTransactionId(), txInput.getPos()));
            List<TxInputSpecial> l = cacheData.queueMapTx.get(txInput.getTransactionId());
            if (l != null) {
                l.remove(txInput);
                if (l.isEmpty()) {
                    cacheData.queueMapTx.remove(txInput.getTransactionId());
                }
            }
        }
    }

    public void update(TxInputSpecial txInput) throws SQLException {
        log.trace("update(txInput:{})", txInput);
        synchronized (cacheData) {
            InOutKey key = new InOutKey(txInput.getTransactionId(), txInput.getPos());
            TxInputSpecial txInput2 = cacheData.queueMap.get(key);
            boolean updatedInQueue = false;
            if (txInput2 != null) {
                if (cacheData.addQueue.remove(txInput2)) {
                    cacheData.addQueue.add(txInput);
                    updatedInQueue = true;
                }
                cacheData.queueMap.put(key, txInput);
                //cacheData.queueMapTx.put(key, txInput);
            }
            if (!updatedInQueue) {
                cacheData.queueUpdate.add(txInput);
            }
        }
        if (cacheData.queueUpdate.size() >= MAX_UPDATE_QUEUE_LENGTH) {
            executeUpdate();
        }
    }

    @SuppressWarnings({"UseSpecificCatch"})
    @Override
    public int executeInserts() {
        Collection<TxInputSpecial> temp = null;
        synchronized (cacheData) {
            if (!cacheData.addQueue.isEmpty()) {
                temp = new ArrayList<>();
                Iterator<TxInputSpecial> it = cacheData.addQueue.iterator();
                for (int i = 0; i < MAX_BATCH_SIZE && it.hasNext(); i++) {
                    temp.add(it.next());
                    it.remove();
                }
                //cacheData.addQueue.clear();
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                BatchExecutor.executeBatch(temp, psAdd.get(), (TxInputSpecial t, PreparedStatement ps) -> {
                    ps.setInt(1, t.getTransactionId());
                    ps.setInt(2, t.getPos());
                    ps.setInt(3, Byte.toUnsignedInt(t.getSighashType()));
                    ps.setBoolean(4, t.isSegwit());
                    ps.setBoolean(5, t.isMultisig());
                });
                synchronized (cacheData) {
                    for (TxInputSpecial t : temp) {
                        cacheData.queueMap.remove(new InOutKey(t.getTransactionId(), t.getPos()));
                        cacheData.queueMapTx.remove(t.getTransactionId());
                    }
                }
            }
        }
        return temp == null ? 0 : temp.size();
    }

    public void executeUpdate() {
        Collection<TxInputSpecial> temp = null;
        synchronized (cacheData) {
            if (!cacheData.queueUpdate.isEmpty()) {
                temp = new ArrayList<>(cacheData.queueUpdate);
                cacheData.queueUpdate.clear();
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                BatchExecutor.executeBatch(temp, psUpdate.get(), (TxInputSpecial t, PreparedStatement ps) -> {
                    ps.setInt(1, Byte.toUnsignedInt(t.getSighashType()));
                    ps.setBoolean(2, t.isSegwit());
                    ps.setBoolean(3, t.isMultisig());
                    ps.setInt(4, t.getTransactionId());
                    ps.setInt(5, t.getPos());
                });
            }
        }
    }

    @Override
    public void close() {
        executeUpdate();
        super.close();
    }

    @Getter
    public static class CacheData {

        private final Collection<TxInputSpecial> addQueue = new LinkedHashSet<>();
        private final Map<InOutKey, TxInputSpecial> queueMap = new HashMap<>();
        private final Map<Integer, List<TxInputSpecial>> queueMapTx = new HashMap<>();
        private final Collection<TxInputSpecial> queueUpdate = new ArrayList<>();
    }
}
