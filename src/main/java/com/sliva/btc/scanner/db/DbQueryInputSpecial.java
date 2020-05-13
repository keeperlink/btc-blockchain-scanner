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
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbQueryInputSpecial {

    private static final int MAX_INS_IN_TXN = 99999;
    private static final String SQL_QUERY_INPUT = "SELECT sighash_type,segwit,multisig FROM input_special WHERE transaction_id=? AND pos=? LIMIT 1";
    private static final String SQL_QUERY_INPUTS = "SELECT pos,sighash_type,segwit,multisig FROM input_special WHERE transaction_id=? ORDER BY pos LIMIT " + MAX_INS_IN_TXN;
    private final DBPreparedStatement psQueryInput;
    private final DBPreparedStatement psQueryInputs;

    public DbQueryInputSpecial(DBConnectionSupplier conn) {
        this.psQueryInput = conn.prepareStatement(SQL_QUERY_INPUT, "input_special.transaction_id");
        this.psQueryInputs = conn.prepareStatement(SQL_QUERY_INPUTS, "input_special.transaction_id");
    }

    @NonNull
    public Optional<TxInputSpecial> getInput(int transactionId, short pos) throws SQLException {
        return psQueryInput
                .setParameters(ps -> ps.setInt(transactionId).setShort(pos))
                .querySingleRow(
                        rs -> TxInputSpecial.builder()
                                .transactionId(transactionId)
                                .pos(pos)
                                .sighashType(rs.getByte(1))
                                .segwit(rs.getBoolean(2))
                                .multisig(rs.getBoolean(3))
                                .build());
    }

    public List<TxInputSpecial> getInputs(int transactionId) throws SQLException {
        return psQueryInputs
                .setParameters(ps -> ps.setInt(transactionId))
                .executeQueryToList(
                        rs -> TxInputSpecial.builder()
                                .transactionId(transactionId)
                                .pos(rs.getShort(1))
                                .sighashType(rs.getByte(2))
                                .segwit(rs.getBoolean(3))
                                .multisig(rs.getBoolean(4))
                                .build());
    }
}
