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

import static com.google.common.base.Preconditions.checkState;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.DbUpdate;
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxOutput;
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
public class DbUpdateOutput extends DbUpdate {

    private static final String TABLE_NAME = "output";
    private static final String SQL_ADD = "INSERT INTO `output`(transaction_id,pos,address_id,amount,spent)VALUES(?,?,?,?,?)";
    private static final String SQL_DELETE = "DELETE FROM `output` WHERE transaction_id=? AND pos=?";
    private static final String SQL_DELETE_ALL_ABOVE_TRANSACTION_ID = "DELETE FROM `output` WHERE transaction_id>?";
//    private static final String SQL_UPDATE_SPENT = "UPDATE `output` SET spent=? WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE_SPENT = "INSERT into `output`(spent,transaction_id,pos,address_id,amount)VALUES(?,?,?,0,0) ON DUPLICATE KEY UPDATE spent=VALUES(spent)";
    private static final String SQL_UPDATE_ADDRESS = "UPDATE `output` SET address_id=? WHERE transaction_id=? AND pos=?";
    private static final String SQL_UPDATE_AMOUNT = "UPDATE `output` SET amount=? WHERE transaction_id=? AND pos=?";
    private final DBPreparedStatement psAdd;
    private final DBPreparedStatement psDelete;
    private final DBPreparedStatement psDeleteAllAboveTransactionId;
    private final DBPreparedStatement psUpdateSpent;
    private final DBPreparedStatement psUpdateAddress;
    private final DBPreparedStatement psUpdateAmount;
    @Getter
    @NonNull
    private final CacheData cacheData = new CacheData();
    private final boolean hasSpentField;

    public DbUpdateOutput(DBConnectionSupplier conn) {
        super(TABLE_NAME, conn);
        this.hasSpentField = conn.getDBMetaData().hasField(TABLE_NAME + ".spent");
        this.psAdd = conn.prepareStatement(hasSpentField ? SQL_ADD : SQL_ADD.replace(",spent", "").replace(",?)", ")"));
        this.psDelete = conn.prepareStatement(SQL_DELETE, "output.transaction_id");
        this.psDeleteAllAboveTransactionId = conn.prepareStatement(SQL_DELETE_ALL_ABOVE_TRANSACTION_ID, "output.transaction_id");
        this.psUpdateSpent = hasSpentField
                ? conn.prepareStatement(SQL_UPDATE_SPENT, "output.transaction_id")
                : conn.prepareNonExecutableStatement(SQL_UPDATE_SPENT, "No 'spent' field in table 'output'");
        this.psUpdateAddress = conn.prepareStatement(SQL_UPDATE_ADDRESS, "output.transaction_id");
        this.psUpdateAmount = conn.prepareStatement(SQL_UPDATE_AMOUNT, "output.transaction_id");
    }

    @Override
    public int getCacheFillPercent() {
        return Math.max(cacheData.addQueue.size() * 100 / getMaxInsertsQueueSize(),
                Math.max(cacheData.queueUpdateAddress.size() * 100 / getMaxUpdatesQueueSize(),
                        Math.max(cacheData.queueUpdateAmount.size() * 100 / getMaxUpdatesQueueSize(),
                                cacheData.queueUpdateSpent.size() * 100 / getMaxUpdatesQueueSize())));
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData.addQueue.size() >= getMinBatchSize() || cacheData.queueUpdateAddress.size() >= getMinBatchSize()
                || cacheData.queueUpdateAmount.size() >= getMinBatchSize() || cacheData.queueUpdateSpent.size() >= getMinBatchSize();
    }

    public void add(TxOutput txOutput) {
        log.trace("add(txOutput:{})", txOutput);
        checkState(isActive(), "Instance has been closed");
        synchronized (cacheData) {
            cacheData.addQueue.add(txOutput);
            cacheData.queueMap.put(txOutput, txOutput);
            cacheData.queueMapTx.computeIfAbsent(txOutput.getTransactionId(), id -> new ArrayList<>(2)).add(txOutput);
        }
        waitFullQueue(cacheData.addQueue, getMaxInsertsQueueSize());
    }

    public boolean delete(TxOutput txOutput) {
        log.trace("delete(txOutput:{})", txOutput);
        checkState(isActive(), "Instance has been closed");
        synchronized (cacheData) {
            boolean result = psDelete.setParameters(p -> p.setInt(txOutput.getTransactionId()).setInt(txOutput.getPos())).executeUpdate() == 1;
            cacheData.addQueue.remove(txOutput);
            cacheData.queueMap.remove(txOutput);
            List<TxOutput> l = cacheData.queueMapTx.get(txOutput.getTransactionId());
            if (l != null) {
                l.remove(txOutput);
                if (l.isEmpty()) {
                    cacheData.queueMapTx.remove(txOutput.getTransactionId());
                }
            }
            return result;
        }
    }

    public int deleteAllAboveTransactionId(int transactionId) {
        log.trace("deleteAllAboveTransactionId(transactionId:{})", transactionId);
        checkState(isActive(), "Instance has been closed");
        synchronized (cacheData) {
            int result = psDeleteAllAboveTransactionId.setParameters(p -> p.setInt(transactionId)).executeUpdate();
            cacheData.addQueue.removeIf(txInput -> txInput.getTransactionId() == transactionId);
            cacheData.queueMap.entrySet().removeIf(e -> e.getKey().getTransactionId() == transactionId);
            cacheData.queueMapTx.entrySet().removeIf(e -> e.getKey() == transactionId);
            return result;
        }
    }

    public void updateSpent(int transactionId, short pos, byte status) {
        log.trace("updateSpent(transactionId:{},pos:{},status:{})", transactionId, pos, status);
        checkState(isActive(), "Instance has been closed");
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
        waitFullQueue(cacheData.queueUpdateSpent, getMaxUpdatesQueueSize());
    }

    public void updateAddress(int transactionId, short pos, int addressId) {
        log.trace("updateAddress(transactionId:{},pos:{},addressId:{})", transactionId, pos, addressId);
        checkState(isActive(), "Instance has been closed");
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
        waitFullQueue(cacheData.queueUpdateAddress, getMaxUpdatesQueueSize());
    }

    public void updateAmount(int transactionId, short pos, long amount) {
        log.trace("updateAmount(transactionId:{},pos:{},amount:{})", transactionId, pos, amount);
        checkState(isActive(), "Instance has been closed");
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
        waitFullQueue(cacheData.queueUpdateAmount, getMaxUpdatesQueueSize());
    }

    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, getMaxBatchSize(),
                (t, p) -> p.setInt(t.getTransactionId()).setInt(t.getPos()).setInt(t.getAddressId()).setLong(t.getAmount()).ignoreExtraParam().setInt(t.getStatus()),
                executed -> {
                    synchronized (cacheData) {
                        executed.stream().peek(cacheData.queueMap::remove).map(InOutKey::getTransactionId).forEach(cacheData.queueMapTx::remove);
                    }
                });
    }

    @Override
    public int executeUpdates() {
        return _executeUpdateSpent()
                + _executeUpdateAddress()
                + _executeUpdateAmount();
    }

    private int _executeUpdateSpent() {
        return executeBatch(cacheData, cacheData.queueUpdateSpent, psUpdateSpent, getMaxBatchSize(),
                (t, p) -> p.setInt(t.getStatus()).setInt(t.getTransactionId()).setInt(t.getPos()), null);
    }

    private int _executeUpdateAddress() {
        return executeBatch(cacheData, cacheData.queueUpdateAddress, psUpdateAddress, getMaxBatchSize(),
                (t, p) -> p.setInt(t.getAddressId()).setInt(t.getTransactionId()).setInt(t.getPos()), null);
    }

    private int _executeUpdateAmount() {
        return executeBatch(cacheData, cacheData.queueUpdateAmount, psUpdateAmount, getMaxBatchSize(),
                (t, p) -> p.setLong(t.getAmount()).setInt(t.getTransactionId()).setInt(t.getPos()), null);
    }

    @Getter
    public static class CacheData {

        /**
         * List of elements queued for insertion into DB. DB Updater will pull
         * and immediately remove elements from this collection in batches, then
         * execute DB "INSERT" statements for pulled batch.
         */
        private final Collection<TxOutput> addQueue = new LinkedHashSet<>();
        /**
         * Map of elements queued for insertion into DB including those that are
         * in processing state (vs. addQueue with do not include in processing
         * elements).
         */
        private final Map<InOutKey, TxOutput> queueMap = new HashMap<>();
        /**
         * Same elements as in queueMap, but differently organized for faster
         * retrieval of all elements with matching transactionId.
         */
        private final Map<Integer, List<TxOutput>> queueMapTx = new HashMap<>();
        /**
         * List of elements queued for update of "spent" flag. DB Updater will
         * pull and immediately remove elements from this collection in batches,
         * then execute DB "UPDATE" statements for pulled batch.
         */
        private final Collection<TxOutput> queueUpdateSpent = new LinkedHashSet<>();
        /**
         * List of elements queued for update of "addressId" value. DB Updater
         * will pull and immediately remove elements from this collection in
         * batches, then execute DB "UPDATE" statements for pulled batch.
         */
        private final Collection<TxOutput> queueUpdateAddress = new LinkedHashSet<>();
        /**
         * List of elements queued for update of "amount" value. DB Updater will
         * pull and immediately remove elements from this collection in batches,
         * then execute DB "UPDATE" statements for pulled batch.
         */
        private final Collection<TxOutput> queueUpdateAmount = new LinkedHashSet<>();
    }
}
