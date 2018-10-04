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

import com.sliva.btc.scanner.db.model.TxInputSpecial;
import com.sliva.btc.scanner.db.model.TxOutput;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbQueryInputSpecial {

    private static final String SQL_QUERY_INPUT = "SELECT sighash_type,segwit,multisig FROM input_special WHERE transaction_id=? AND pos=?";
    private static final String SQL_QUERY_INPUTS = "SELECT pos,sighash_type,segwit,multisig FROM input_special WHERE transaction_id=? ORDER BY pos";
    private final ThreadLocal<PreparedStatement> psQueryInput;
    private final ThreadLocal<PreparedStatement> psQueryInputs;

    public DbQueryInputSpecial(DBConnection conn) {
        this.psQueryInput = conn.prepareStatement(SQL_QUERY_INPUT);
        this.psQueryInputs = conn.prepareStatement(SQL_QUERY_INPUTS);
    }

    public TxInputSpecial getInput(int transactionId, short pos) throws SQLException {
        psQueryInput.get().setInt(1, transactionId);
        psQueryInput.get().setShort(2, pos);
        try (ResultSet rs = psQueryInput.get().executeQuery()) {
            if (rs.next()) {
                return TxInputSpecial.builder()
                        .transactionId(transactionId)
                        .pos(pos)
                        .sighashType(rs.getByte(1))
                        .segwit(rs.getBoolean(2))
                        .multisig(rs.getBoolean(3))
                        .build();
            }
        }
        return null;
    }

    public List<TxInputSpecial> getInputs(int transactionId) throws SQLException {
        psQueryInputs.get().setInt(1, transactionId);
        List<TxInputSpecial> result = new ArrayList<>();
        try (ResultSet rs = psQueryInputs.get().executeQuery()) {
            while (rs.next()) {
                result.add(TxInputSpecial.builder()
                        .transactionId(transactionId)
                        .pos(rs.getShort(1))
                        .sighashType(rs.getByte(2))
                        .segwit(rs.getBoolean(3))
                        .multisig(rs.getBoolean(4))
                        .build());
            }
        }
        log.trace("getInputs(transactionId:{}): result={}", transactionId, result);
        return result;
    }

    @Getter
    @Builder
    @ToString
    public static class TxInputOutput {

        private final TxInputSpecial input;
        private final TxOutput output;
    }
}
