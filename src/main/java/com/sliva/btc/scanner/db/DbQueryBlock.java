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

import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.util.Utils;
import java.util.Optional;
import lombok.NonNull;

/**
 *
 * @author Sliva Co
 */
public class DbQueryBlock {

    private static final String SQL_QUERY_BLOCK_HASH = "SELECT `hash`,txn_count FROM `block` WHERE `height`=? LIMIT 1";
    private static final String SQL_FIND_BLOCK_BY_HASH = "SELECT `height`,txn_count FROM `block` WHERE `hash`=? LIMIT 1";
    private static final String SQL_FIND_LAST_HEIGHT = "SELECT `height` FROM `block` ORDER BY `height` DESC LIMIT 1";
    private final DBPreparedStatement psQueryBlockHash;
    private final DBPreparedStatement psFindBlockByHash;
    private final DBPreparedStatement psFindLastHeight;

    public DbQueryBlock(DBConnectionSupplier conn) {
        this.psQueryBlockHash = conn.prepareStatement(SQL_QUERY_BLOCK_HASH, "block.height");
        this.psFindBlockByHash = conn.prepareStatement(SQL_FIND_BLOCK_BY_HASH, "block.hash");
        this.psFindLastHeight = conn.prepareStatement(SQL_FIND_LAST_HEIGHT, "block.height");
    }

    @NonNull
    public Optional<byte[]> getBlockHash(int blockHeight) {
        return psQueryBlockHash.setParameters(ps -> ps.setInt(blockHeight)).querySingleRow(rs -> rs.getBytes(1));
    }

    @NonNull
    public Optional<BtcBlock> getBlock(int blockHeight) {
        return psQueryBlockHash.setParameters(ps -> ps.setInt(blockHeight)).querySingleRow(rs -> BtcBlock.builder()
                .height(blockHeight)
                .hash(rs.getBytes(1))
                .txnCount(rs.getInt(2))
                .build());
    }

    @NonNull
    public Optional<BtcBlock> findBlockByHash(String hash) {
        byte[] binHash = Utils.id2bin(hash);
        return psFindBlockByHash.setParameters(ps -> ps.setBytes(binHash)).querySingleRow(rs -> BtcBlock.builder()
                .height(rs.getInt(1))
                .hash(binHash)
                .txnCount(rs.getInt(2))
                .build());
    }

    @NonNull
    public Optional<Integer> findLastHeight() {
        return DBUtils.readInteger(psFindLastHeight);
    }
}
