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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
public class DbQueryBlock {

    private static final String SQL_QUERY_BLOCK_HASH = "SELECT hash,txn_count FROM block WHERE height=?";
    private static final String SQL_FIND_BLOCK_BY_HASH = "SELECT height,txn_count FROM block WHERE hash=?";
    private static final String SQL_FIND_LAST_HEIGHT = "SELECT height FROM block ORDER BY height DESC LIMIT 1";
    private final ThreadLocal<PreparedStatement> psQueryBlockHash;
    private final ThreadLocal<PreparedStatement> psFindBlockByHash;
    private final ThreadLocal<PreparedStatement> psFindLastHeight;

    public DbQueryBlock(DBConnection conn) {
        this.psQueryBlockHash = conn.prepareStatement(SQL_QUERY_BLOCK_HASH);
        this.psFindBlockByHash = conn.prepareStatement(SQL_FIND_BLOCK_BY_HASH);
        this.psFindLastHeight = conn.prepareStatement(SQL_FIND_LAST_HEIGHT);
    }

    public byte[] getBlockHash(int blockHeight) throws SQLException {
        psQueryBlockHash.get().setInt(1, blockHeight);
        try (ResultSet rs = psQueryBlockHash.get().executeQuery()) {
            return rs.next() ? rs.getBytes(1) : null;
        }
    }

    public BtcBlock getBlock(int blockHeight) throws SQLException {
        psQueryBlockHash.get().setInt(1, blockHeight);
        try (ResultSet rs = psQueryBlockHash.get().executeQuery()) {
            return rs.next() ? BtcBlock.builder()
                    .height(blockHeight)
                    .hash(Hex.encodeHexString(rs.getBytes(1)))
                    .txnCount(rs.getInt(2))
                    .build() : null;
        }
    }

    public BtcBlock findBlockByHash(String hash) throws SQLException {
        psFindBlockByHash.get().setBytes(1, Utils.id2bin(hash));
        try (ResultSet rs = psFindBlockByHash.get().executeQuery()) {
            return rs.next() ? BtcBlock.builder()
                    .height(rs.getInt(1))
                    .hash(hash)
                    .txnCount(rs.getInt(2))
                    .build() : null;
        }
    }

    public int findLastHeight() throws SQLException {
        try (ResultSet rs = psFindLastHeight.get().executeQuery()) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }
}
