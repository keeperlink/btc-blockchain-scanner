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

import static com.sliva.btc.scanner.util.Utils.id2hex;
import java.util.Collection;
import java.util.stream.Stream;

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
            txid = blockProvider.psQueryTransactionHash
                    .setParameters(p -> p.setInt(transactionId))
                    .querySingleRow(rs -> id2hex(rs.getBytes(1)))
                    .orElseThrow(() -> new IllegalStateException("Transaction #" + transactionId + " not found in DB"));
        }
        return txid;
    }

    @Override
    public Stream<DbInput> getInputs() {
        if (inputs == null) {
            inputs = blockProvider.psQueryTransactionInputs
                    .setParameters(p -> p.setInt(transactionId))
                    .executeQueryToList(rs -> new DbInput(blockProvider, rs.getShort(1), rs.getShort(2), rs.getInt(3), null, rs.getByte(4), rs.getBoolean(5), rs.getBoolean(6)));
        }
        return inputs.stream();
    }

    @Override
    public Stream<DbOutput> getOutputs() {
        if (outputs == null) {
            outputs = blockProvider.psQueryTransactionOutputs
                    .setParameters(p -> p.setInt(transactionId))
                    .executeQueryToList(rs -> new DbOutput(blockProvider, rs.getShort(1), rs.getInt(2), rs.getLong(3), rs.getByte(4)));
        }
        return outputs.stream();
    }

    public int getTransactionId() {
        return transactionId;
    }

}
