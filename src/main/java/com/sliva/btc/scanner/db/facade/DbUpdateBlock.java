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
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.DbUpdate;
import com.sliva.btc.scanner.db.model.BtcBlock;
import static com.sliva.btc.scanner.util.Utils.getPercentage;
import java.util.Collection;
import java.util.LinkedHashSet;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbUpdateBlock extends DbUpdate {

    private static final String TABLE_NAME = "block";
    private static final String SQL_ADD = "INSERT INTO `block`(`height`,`hash`,txn_count)VALUES(?,?,?)";
    private static final String SQL_DELETE = "DELETE FROM `block` WHERE height=?";
    private final DBPreparedStatement psAdd;
    private final DBPreparedStatement psDelete;
    @Getter
    @NonNull
    private final CacheData cacheData;

    public DbUpdateBlock(DBConnectionSupplier conn) {
        this(conn, new CacheData());
    }

    public DbUpdateBlock(DBConnectionSupplier conn, CacheData cacheData) {
        super(TABLE_NAME, conn);
        checkArgument(cacheData != null, "Argument 'cacheData' is null");
        this.psAdd = conn.prepareStatement(SQL_ADD);
        this.psDelete = conn.prepareStatement(SQL_DELETE, "block.height");
        this.cacheData = cacheData;
    }

    @Override
    public int getCacheFillPercent() {
        return getPercentage(cacheData.addQueue.size(), getMaxInsertsQueueSize());
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData.addQueue.size() >= getMinBatchSize();
    }

    public void add(BtcBlock btcBlock) {
        log.trace("add(btcBlock:{})", btcBlock);
        checkState(isActive(), "Instance has been closed");
        waitFullQueue(cacheData.addQueue, getMaxInsertsQueueSize());
        synchronized (cacheData) {
            cacheData.addQueue.add(btcBlock);
        }
    }

    public boolean delete(BtcBlock btcBlock) {
        log.trace("delete(btcBlock:{})", btcBlock);
        checkState(isActive(), "Instance has been closed");
        synchronized (cacheData) {
            cacheData.addQueue.remove(btcBlock);
        }
        return psDelete.setParameters(p -> p.setInt(btcBlock.getHeight())).executeUpdate() == 1;
    }

    public boolean delete(int blockHeight) {
        return delete(BtcBlock.builder().height(blockHeight).build());
    }

    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, getMaxBatchSize(),
                (t, ps) -> ps.setInt(t.getHeight()).setBytes(t.getHash().getData()).setInt(t.getTxnCount()), null);
    }

    @Override
    public int executeUpdates() {
        return 0;
    }

    @Getter
    public static class CacheData {

        private final Collection<BtcBlock> addQueue = new LinkedHashSet<>();
    }
}
