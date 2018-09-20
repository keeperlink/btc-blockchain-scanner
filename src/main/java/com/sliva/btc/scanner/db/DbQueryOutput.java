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

import com.sliva.btc.scanner.db.model.TxInput;
import com.sliva.btc.scanner.db.model.TxOutput;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 *
 * @author Sliva Co
 */
public class DbQueryOutput {

    private static final String SQL_QUERY_OUTPUTS = "SELECT pos,address_id,amount,spent FROM output WHERE transaction_id=?";
    private static final String SQL_QUERY_OUTPUT = "SELECT address_id,amount,spent FROM output WHERE transaction_id=? AND pos=?";
    private static final String SQL_QUERY_OUTPUTS_WITH_INPUT = "SELECT O.pos,O.address_id,O.amount,O.spent"
            + ",I.transaction_id,I.pos"
            + " FROM output O"
            + " LEFT JOIN input I ON I.in_transaction_id=O.transaction_id AND I.in_pos=O.pos"
            + " WHERE O.transaction_id=?";
    private final ThreadLocal<PreparedStatement> psQueryOutputs;
    private final ThreadLocal<PreparedStatement> psQueryOutput;
    private final ThreadLocal<PreparedStatement> psQueryOutputsWithInput;

    public DbQueryOutput(DBConnection conn) throws SQLException {
        this.psQueryOutputs = conn.prepareStatement(SQL_QUERY_OUTPUTS);
        this.psQueryOutput = conn.prepareStatement(SQL_QUERY_OUTPUT);
        this.psQueryOutputsWithInput = conn.prepareStatement(SQL_QUERY_OUTPUTS_WITH_INPUT);
    }

    public List<TxOutput> getOutputs(int transactionId) throws SQLException {
        psQueryOutputs.get().setInt(1, transactionId);
        try (ResultSet rs = psQueryOutputs.get().executeQuery()) {
            List<TxOutput> result = null;
            while (rs.next()) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(TxOutput.builder()
                        .transactionId(transactionId)
                        .pos(rs.getInt(1))
                        .addressId(rs.getInt(2))
                        .amount(rs.getLong(3))
                        .status(rs.getInt(4))
                        .build());
            }
            return result;
        }
    }

    public TxOutput getOutput(int transactionId, int pos) throws SQLException {
        psQueryOutput.get().setInt(1, transactionId);
        psQueryOutput.get().setInt(2, pos);
        try (ResultSet rs = psQueryOutput.get().executeQuery()) {
            return rs.next() ? TxOutput.builder()
                    .transactionId(transactionId)
                    .pos(pos)
                    .addressId(rs.getInt(1))
                    .amount(rs.getLong(2))
                    .status(rs.getInt(3))
                    .build() : null;
        }
    }

    public List<TxOutputInput> getOutputsWithInput(int transactionId) throws SQLException {
        psQueryOutputsWithInput.get().setInt(1, transactionId);
        try (ResultSet rs = psQueryOutputsWithInput.get().executeQuery()) {
            List<TxOutputInput> result = null;
            while (rs.next()) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                TxOutputInput.TxOutputInputBuilder builder = TxOutputInput.builder();
                builder.output(TxOutput.builder()
                        .transactionId(transactionId)
                        .pos(rs.getInt(1))
                        .addressId(rs.getInt(2))
                        .amount(rs.getLong(3))
                        .status(rs.getInt(4))
                        .build());
                if (rs.getObject(5) != null) {
                    builder.input(TxInput.builder()
                            .transactionId(rs.getInt(5))
                            .pos(rs.getInt(6))
                            .inTransactionId(transactionId)
                            .inPos(rs.getInt(1))
                            .build());
                }
                result.add(builder.build());
            }
            return result;
        }
    }

    @Getter
    @Builder
    @ToString
    public static class TxOutputInput {

        private final TxOutput output;
        private final TxInput input;
    }
}
