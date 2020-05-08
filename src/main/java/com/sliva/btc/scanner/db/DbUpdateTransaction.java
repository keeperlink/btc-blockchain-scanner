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
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.util.Utils;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateTransaction extends DbUpdate {

    private static int MIN_BATCH_SIZE = 1;
    public static int MAX_BATCH_SIZE = 20000;
    public static int MAX_INSERT_QUEUE_LENGTH = 1000000;
    private static int MAX_UPDATE_QUEUE_LENGTH = 1000;
    private static final String TABLE_NAME = "transaction";
    private static final String SQL_ADD = "INSERT INTO transaction(transaction_id,txid,block_height,nInputs,nOutputs)VALUES(?,?,?,?,?)";
    private static final String SQL_DELETE = "DELETE FROM transaction WHERE transaction_id=?";
    private static final String SQL_UPDATE_IN_OUT = "UPDATE transaction SET nInputs=?,nOutputs=? WHERE transaction_id=?";
    private final ThreadLocal<PreparedStatement> psAdd;
    private final ThreadLocal<PreparedStatement> psDelete;
    private final ThreadLocal<PreparedStatement> psUpdateInOut;
    private final CacheData cacheData;

    public DbUpdateTransaction(DBConnection conn) {
        this(conn, new CacheData());
    }

    public DbUpdateTransaction(DBConnection conn, CacheData cacheData) {
        super(conn);
        this.psAdd = conn.prepareStatement(SQL_ADD);
        this.psDelete = conn.prepareStatement(SQL_DELETE);
        this.psUpdateInOut = conn.prepareStatement(SQL_UPDATE_IN_OUT);
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
        return cacheData == null ? 0 : Math.max(cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH, cacheData.updateInOutQueue.size() * 100 / MAX_UPDATE_QUEUE_LENGTH);
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData != null && (cacheData.addQueue.size() >= MIN_BATCH_SIZE || cacheData.updateInOutQueue.size() >= MIN_BATCH_SIZE);
    }

    public void add(BtcTransaction tx) throws SQLException {
        log.trace("add(t:{})", tx);
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        synchronized (cacheData) {
            cacheData.addQueue.add(tx);
            cacheData.addMap.put(tx.getTxid(), tx);
            cacheData.addMapId.put(tx.getTransactionId(), tx);
        }
    }

    public void delete(BtcTransaction tx) throws SQLException {
        log.trace("delete(tx:{})", tx);
        synchronized (cacheData) {
            psDelete.get().setInt(1, tx.getTransactionId());
            psDelete.get().execute();
            cacheData.addQueue.remove(tx);
            cacheData.addMap.remove(tx.getTxid());
            cacheData.addMapId.remove(tx.getTransactionId());
        }
    }

    @Deprecated
    public void updateInOut(int transactionId, int nInputs, int nOutputs) throws SQLException {
        throw new UnsupportedOperationException();
//        synchronized (cacheData) {
//            //TODO need to check add queue first
//            cacheData.updateInOutQueue.add(BtcTransaction.builder()
//                    .transactionId(transactionId)
//                    .nInputs(nInputs)
//                    .nOutputs(nOutputs)
//                    .build());
//        }
//        if (cacheData.updateInOutQueue.size() >= MAX_UPDATE_QUEUE_LENGTH) {
//            executeUpdateInOuts();
//        }
    }

    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, MAX_BATCH_SIZE, (t, ps) -> {
            ps.setInt(1, t.getTransactionId());
            ps.setBytes(2, Utils.id2bin(t.getTxid()));
            ps.setInt(3, t.getBlockHeight());
            ps.setInt(4, t.getNInputs());
            ps.setInt(5, t.getNOutputs());
        }, executed -> {
            synchronized (cacheData) {
                executed.stream().peek(t -> cacheData.addMap.remove(t.getTxid())).map(BtcTransaction::getTransactionId).forEach(cacheData.addMapId::remove);
            }
        });
    }

    @Override
    public int executeUpdates() {
        return _executeUpdateInOuts();
    }

    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    private int _executeUpdateInOuts() {
        return executeBatch(cacheData, cacheData.updateInOutQueue, psUpdateInOut, MAX_BATCH_SIZE, (t, ps) -> {
            ps.setInt(1, t.getNInputs());
            ps.setInt(2, t.getNOutputs());
            ps.setInt(3, t.getTransactionId());
        }, null);
    }

    @Getter
    public static class CacheData {

        private final Collection<BtcTransaction> addQueue = new LinkedHashSet<>();
        private final Map<String, BtcTransaction> addMap = new HashMap<>();
        private final Map<Integer, BtcTransaction> addMapId = new HashMap<>();
        private final Set<BtcTransaction> updateInOutQueue = new HashSet<>();
    }
}
