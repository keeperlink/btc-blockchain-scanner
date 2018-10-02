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

import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxOutput;
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
public class DbUpdateOutput extends DbUpdate {

    public static int MIN_BATCH_SIZE = 1;
    public static int MAX_BATCH_SIZE = 40000;
    public static int MAX_INSERT_QUEUE_LENGTH = 1000000;
    public static int MAX_UPDATE_QUEUE_LENGTH = 10000;
    private static final String TABLE_NAME = "output";
    private static final String SQL_ADD = "INSERT INTO output(transaction_id,pos,address_id,amount,spent)VALUES(?,?,?,?,?)";
    private static final String SQL_DELETE = "DELETE FROM output WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE_SPENT = "UPDATE output SET spent=? WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE_ADDRESS = "UPDATE output SET address_id=? WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE_AMOUNT = "UPDATE output SET amount=? WHERE transaction_id=? AND pos=?";
    private final ThreadLocal<PreparedStatement> psAdd;
    private final ThreadLocal<PreparedStatement> psDelete;
    private final ThreadLocal<PreparedStatement> psUpdateSpent;
    private final ThreadLocal<PreparedStatement> psUpdateAddress;
    private final ThreadLocal<PreparedStatement> psUpdateAmount;
    private final CacheData cacheData;

    public DbUpdateOutput(DBConnection conn) {
        this(conn, new CacheData());
    }

    public DbUpdateOutput(DBConnection conn, CacheData cacheData) {
        super(conn);
        this.psAdd = conn.prepareStatement(SQL_ADD);
        this.psDelete = conn.prepareStatement(SQL_DELETE);
        this.psUpdateSpent = conn.prepareStatement(SQL_UPDATE_SPENT);
        this.psUpdateAddress = conn.prepareStatement(SQL_UPDATE_ADDRESS);
        this.psUpdateAmount = conn.prepareStatement(SQL_UPDATE_AMOUNT);
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

    public void add(TxOutput txOutput) {
        log.trace("add(txOutput:{})", txOutput);
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        synchronized (cacheData) {
            cacheData.addQueue.add(txOutput);
            cacheData.queueMap.put(new InOutKey(txOutput.getTransactionId(), txOutput.getPos()), txOutput);
            List<TxOutput> list = cacheData.queueMapTx.get(txOutput.getTransactionId());
            if (list == null) {
                cacheData.queueMapTx.put(txOutput.getTransactionId(), list = new ArrayList<>());
            }
            list.add(txOutput);
        }
    }

    public void delete(TxOutput txOutput) throws SQLException {
        log.trace("delete(txOutput:{})", txOutput);
        synchronized (cacheData) {
            psDelete.get().setInt(1, txOutput.getTransactionId());
            psDelete.get().setInt(2, txOutput.getPos());
            psDelete.get().execute();
            cacheData.addQueue.remove(txOutput);
            cacheData.queueMap.remove(new InOutKey(txOutput.getTransactionId(), txOutput.getPos()));
            List<TxOutput> l = cacheData.queueMapTx.get(txOutput.getTransactionId());
            if (l != null) {
                l.remove(txOutput);
                if (l.isEmpty()) {
                    cacheData.queueMapTx.remove(txOutput.getTransactionId());
                }
            }
        }
    }

    public void updateSpent(int transactionId, int pos, int status) throws SQLException {
        log.trace("updateSpent(transactionId:{},pos:{},status:{})", transactionId, pos, status);
        synchronized (cacheData) {
            InOutKey pk = new InOutKey(transactionId, pos);
            TxOutput txOutput = cacheData.queueMap.get(pk);
            boolean updatedInQueue = false;
            if (txOutput != null) {
                if (txOutput.getStatus() != status) {
                    txOutput = txOutput.toBuilder().status(status).build();
                    if (cacheData.addQueue.remove(txOutput)) {
                        cacheData.addQueue.add(txOutput);
                        updatedInQueue = true;
                    }
                } else {
                    //value not changed
                    updatedInQueue = true;
                }
                cacheData.queueMap.put(pk, txOutput);
            }
            if (!updatedInQueue) {
                cacheData.queueUpdateSpent.add(TxOutput.builder().transactionId(transactionId).pos(pos).status(status).build());
            }
        }
        if (cacheData.queueUpdateSpent.size() >= MAX_UPDATE_QUEUE_LENGTH) {
            executeUpdateSpent();
        }
    }

    public void updateAddress(int transactionId, int pos, int addressId) throws SQLException {
        log.trace("updateAddress(transactionId:{},pos:{},addressId:{})", transactionId, pos, addressId);
        synchronized (cacheData) {
            InOutKey pk = new InOutKey(transactionId, pos);
            TxOutput txOutput = cacheData.queueMap.get(pk);
            boolean updatedInQueue = false;
            if (txOutput != null) {
                if (txOutput.getAddressId() != addressId) {
                    txOutput = txOutput.toBuilder().addressId(addressId).build();
                    if (cacheData.addQueue.remove(txOutput)) {
                        cacheData.addQueue.add(txOutput);
                        updatedInQueue = true;
                    }
                } else {
                    //value not changed
                    updatedInQueue = true;
                }
                cacheData.queueMap.put(pk, txOutput);
            }
            if (!updatedInQueue) {
                cacheData.queueUpdateAddress.add(TxOutput.builder().transactionId(transactionId).pos(pos).addressId(addressId).build());
            }
        }
        if (cacheData.queueUpdateAddress.size() >= MAX_UPDATE_QUEUE_LENGTH) {
            executeUpdateAddress();
        }
    }

    public void updateAmount(int transactionId, int pos, long amount) throws SQLException {
        log.trace("updateAmount(transactionId:{},pos:{},amount:{})", transactionId, pos, amount);
        synchronized (cacheData) {
            InOutKey pk = new InOutKey(transactionId, pos);
            TxOutput txOutput = cacheData.queueMap.get(pk);
            boolean updatedInQueue = false;
            if (txOutput != null) {
                if (txOutput.getAmount() != amount) {
                    txOutput = txOutput.toBuilder().amount(amount).build();
                    if (cacheData.addQueue.remove(txOutput)) {
                        cacheData.addQueue.add(txOutput);
                        updatedInQueue = true;
                    }
                } else {
                    //value not changed
                    updatedInQueue = true;
                }
                cacheData.queueMap.put(pk, txOutput);
            }
            if (!updatedInQueue) {
                cacheData.queueUpdateAmount.add(TxOutput.builder().transactionId(transactionId).pos(pos).amount(amount).build());
            }
        }
        if (cacheData.queueUpdateAddress.size() >= MAX_UPDATE_QUEUE_LENGTH) {
            executeUpdateAddress();
        }
    }

    @Override
    public int executeInserts() {
        Collection<TxOutput> temp = null;
        synchronized (cacheData) {
            if (!cacheData.addQueue.isEmpty()) {
                temp = new ArrayList<>();
                Iterator<TxOutput> it = cacheData.addQueue.iterator();
                for (int i = 0; i < MAX_BATCH_SIZE && it.hasNext(); i++) {
                    temp.add(it.next());
                    it.remove();
                }
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                BatchExecutor.executeBatch(temp, psAdd.get(), (TxOutput t, PreparedStatement ps) -> {
                    ps.setInt(1, t.getTransactionId());
                    ps.setInt(2, t.getPos());
                    ps.setInt(3, t.getAddressId());
                    ps.setLong(4, t.getAmount());
                    ps.setInt(5, t.getStatus());
                });
                synchronized (cacheData) {
                    for (TxOutput t : temp) {
                        cacheData.queueMap.remove(new InOutKey(t.getTransactionId(), t.getPos()));
                        cacheData.queueMapTx.remove(t.getTransactionId());
                    }
                }
            }
        }
        return temp == null ? 0 : temp.size();
    }

    public void executeUpdateSpent() {
        Collection<TxOutput> temp = null;
        synchronized (cacheData) {
            if (!cacheData.queueUpdateSpent.isEmpty()) {
                temp = new ArrayList<>(cacheData.queueUpdateSpent);
                cacheData.queueUpdateSpent.clear();
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                long s = System.currentTimeMillis();
                BatchExecutor.executeBatch(temp, psUpdateSpent.get(), (TxOutput t, PreparedStatement ps) -> {
                    ps.setInt(1, t.getStatus());
                    ps.setInt(2, t.getTransactionId());
                    ps.setInt(3, t.getPos());
                });
                log.debug("executeUpdateSpent({}): runtime={}", temp.size(), (System.currentTimeMillis() - s) + " ms.");
            }
        }
    }

    public void executeUpdateAddress() {
        Collection<TxOutput> temp = null;
        synchronized (cacheData) {
            if (!cacheData.queueUpdateAddress.isEmpty()) {
                temp = new ArrayList<>(cacheData.queueUpdateAddress);
                cacheData.queueUpdateAddress.clear();
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                long s = System.currentTimeMillis();
                BatchExecutor.executeBatch(temp, psUpdateAddress.get(), (TxOutput t, PreparedStatement ps) -> {
                    ps.setInt(1, t.getAddressId());
                    ps.setInt(2, t.getTransactionId());
                    ps.setInt(3, t.getPos());
                });
                log.debug("executeUpdateAddress({}): runtime={}", temp.size(), (System.currentTimeMillis() - s) + " ms.");
            }
        }
    }

    public void executeUpdateAmount() {
        Collection<TxOutput> temp = null;
        synchronized (cacheData) {
            if (!cacheData.queueUpdateAmount.isEmpty()) {
                temp = new ArrayList<>(cacheData.queueUpdateAmount);
                cacheData.queueUpdateAmount.clear();
            }
        }
        if (temp != null) {
            synchronized (execSync) {
                long s = System.currentTimeMillis();
                BatchExecutor.executeBatch(temp, psUpdateAmount.get(), (TxOutput t, PreparedStatement ps) -> {
                    ps.setLong(1, t.getAmount());
                    ps.setInt(2, t.getTransactionId());
                    ps.setInt(3, t.getPos());
                });
                log.debug("executeUpdateAmount({}): runtime={}", temp.size(), (System.currentTimeMillis() - s) + " ms.");
            }
        }
    }

    @Override
    public void flushCache() {
        log.trace("flushCache() Called");
        super.flushCache();
        executeUpdateSpent();
        executeUpdateAddress();
        executeUpdateAmount();
    }

    @Getter
    public static class CacheData {

        private final Collection<TxOutput> addQueue = new LinkedHashSet<>();
        private final Map<InOutKey, TxOutput> queueMap = new HashMap<>();
        private final Map<Integer, List<TxOutput>> queueMapTx = new HashMap<>();
        private final Collection<TxOutput> queueUpdateSpent = new ArrayList<>();
        private final Collection<TxOutput> queueUpdateAddress = new ArrayList<>();
        private final Collection<TxOutput> queueUpdateAmount = new ArrayList<>();
    }
}
