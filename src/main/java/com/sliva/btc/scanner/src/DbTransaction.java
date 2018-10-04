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
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
public class DbTransaction implements SrcTransaction<DbInput, DbOutput> {

    private final DbBlockProvider blockProvider;
    private final int transactionId;
    private String txid;
    private Collection<DbInput> inputs;
    private Collection<DbOutput> outputs;

    public DbTransaction(DbBlockProvider blockProvider, int transactionId, String txid) {
        this.blockProvider = blockProvider;
        this.transactionId = transactionId;
        this.txid = txid;
    }

    @Override
    public String getTxid() {
        if (txid == null) {
            try {
                blockProvider.psQueryTransactionHash.get().setInt(1, transactionId);
                try (ResultSet rs = blockProvider.psQueryTransactionHash.get().executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Transaction #" + transactionId + " not found in DB");
                    }
                    txid = Hex.encodeHexString(rs.getBytes(1), true);
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return txid;
    }

    @Override
    public Stream<DbInput> getInputs() {
        if (inputs == null) {
            try {
                blockProvider.psQueryTransactionInputs.get().setInt(1, transactionId);
                try (ResultSet rs = blockProvider.psQueryTransactionInputs.get().executeQuery()) {
                    Collection<DbInput> t = new ArrayList<>();
                    while (rs.next()) {
                        t.add(new DbInput(blockProvider, rs.getShort(1), rs.getShort(2), rs.getInt(3), null, rs.getByte(4), rs.getBoolean(5), rs.getBoolean(6)));
                    }
                    inputs = t;
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return inputs.stream();
    }

    @Override
    public Stream<DbOutput> getOutputs() {
        if (outputs == null) {
            try {
                blockProvider.psQueryTransactionOutputs.get().setInt(1, transactionId);
                try (ResultSet rs = blockProvider.psQueryTransactionOutputs.get().executeQuery()) {
                    Collection<DbOutput> t = new ArrayList<>();
                    while (rs.next()) {
                        t.add(new DbOutput(blockProvider, rs.getShort(1), rs.getInt(2), rs.getLong(3), rs.getByte(4)));
                    }
                    outputs = t;
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return outputs.stream();
    }

    public int getTransactionId() {
        return transactionId;
    }

}
