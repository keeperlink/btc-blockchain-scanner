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
import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.util.Utils;
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

    private static int MIN_BATCH_SIZE = 1;
    private static int MAX_BATCH_SIZE = 10000;
    private static int MAX_INSERT_QUEUE_LENGTH = 30000;
    private static final String TABLE_NAME = "block";
    private static final String SQL_ADD = "INSERT INTO `block`(`height`,`hash`,txn_count)VALUES(?,?,?)";
    private final DBPreparedStatement psAdd;
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
        this.cacheData = cacheData;
    }

    @Override
    public int getCacheFillPercent() {
        return cacheData.addQueue.size() * 100 / MAX_INSERT_QUEUE_LENGTH;
    }

    @Override
    public boolean isExecuteNeeded() {
        return cacheData.addQueue.size() >= MIN_BATCH_SIZE;
    }

    public void add(BtcBlock btcBlock) {
        log.trace("add(btcBlock:{})", btcBlock);
        checkState(isActive(), "Instance has been closed");
        waitFullQueue(cacheData.addQueue, MAX_INSERT_QUEUE_LENGTH);
        synchronized (cacheData) {
            cacheData.addQueue.add(btcBlock);
        }
    }

    @Override
    public int executeInserts() {
        return executeBatch(cacheData, cacheData.addQueue, psAdd, MAX_BATCH_SIZE,
                (t, ps) -> ps.setInt(t.getHeight()).setBytes(Utils.id2bin(t.getHash())).setInt(t.getTxnCount()), null);
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
