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
package com.sliva.btc.scanner.src;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
public class DbInput implements SrcInput {

    private final DbBlockProvider blockProvider;
    private final int pos;
    private final int inPos;
    private final int inTransactionId;
    private String inTxid;

    public DbInput(DbBlockProvider blockProvider, int pos, int inPos, int inTransactionId) {
        this.blockProvider = blockProvider;
        this.pos = pos;
        this.inPos = inPos;
        this.inTransactionId = inTransactionId;
    }

    @Override
    public int getPos() {
        return pos;
    }

    @Override
    public String getInTxid() {
        if (inTxid == null) {
            try {
                blockProvider.psQueryTransactionHash.get().setInt(1, inTransactionId);
                try (ResultSet rs = blockProvider.psQueryTransactionHash.get().executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Transaction #" + inTransactionId + " not found in DB");
                    }
                    inTxid = Hex.encodeHexString(rs.getBytes(1), true);
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return inTxid;
    }

    @Override
    public int getInPos() {
        return inPos;
    }

    public int getInTransactionId() {
        return inTransactionId;
    }

}
