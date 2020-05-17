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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.TxInput;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.db.utils.DBUtils;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class DbQueryInput {

    private static final int MAX_INS_IN_TXN = 99999;
    private static final String SQL_QUERY_INPUTS = "SELECT pos,in_transaction_id,in_pos FROM input WHERE transaction_id=? ORDER BY pos LIMIT " + MAX_INS_IN_TXN;
    private static final String SQL_COUNT_INPUTS_IN_TX = "SELECT count(*) FROM input WHERE transaction_id=? LIMIT 1";
    private static final String SQL_FIND_INPUT_BY_OUT_TX = "SELECT transaction_id,pos FROM input WHERE in_transaction_id=? AND in_pos=? LIMIT 1";
    private static final String SQL_QUERY_INPUTS_WITH_OUTPUT = "SELECT"
            + " I.pos,I.in_transaction_id,I.in_pos"
            + ",O.address_id,O.amount,spent"
            + " FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos = I.in_pos"
            + " WHERE I.transaction_id=? ORDER BY I.pos LIMIT " + MAX_INS_IN_TXN;
    private static final String SQL_QUERY_INPUT_ADDRESSES
            = "SELECT O.address_id,IFNULL(P2PKH.wallet_id, IFNULL(P2SH.wallet_id, IFNULL(P2WPKH.wallet_id, P2WSH.wallet_id)))"
            + " FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " LEFT JOIN address_p2pkh P2PKH ON P2PKH.address_id=O.address_id"
            + " LEFT JOIN address_p2sh P2SH ON P2SH.address_id=O.address_id"
            + " LEFT JOIN address_p2wpkh P2WPKH ON P2WPKH.address_id=O.address_id"
            + " LEFT JOIN address_p2wsh P2WSH ON P2WSH.address_id=O.address_id"
            + " WHERE O.address_id>0 AND I.transaction_id=? LIMIT " + MAX_INS_IN_TXN;
//    private static final String SQL_QUERY_TRANSACTION_IDS_ABOVE = "SELECT DISTINCT transaction_id FROM input WHERE transaction_id>? LIMIT " + MAX_INS_IN_TXN;
    private final DBPreparedStatement psQueryInputs;
    private final DBPreparedStatement psCountInputsInTx;
    private final DBPreparedStatement psFindInputByOutTx;
    private final DBPreparedStatement psQueryInputsWithOutput;
    private final DBPreparedStatement psQueryInputAddresses;
//    private final DBPreparedStatement psQueryTransactionIdsAbove;
    private final boolean hasSpentField;

    public DbQueryInput(DBConnectionSupplier conn) {
        this.hasSpentField = conn.getDBMetaData().hasField("output.spent");
        this.psQueryInputs = conn.prepareStatement(SQL_QUERY_INPUTS, "input.transaction_id");
        this.psCountInputsInTx = conn.prepareStatement(SQL_COUNT_INPUTS_IN_TX, "input.transaction_id");
        this.psFindInputByOutTx = conn.prepareStatement(SQL_FIND_INPUT_BY_OUT_TX, "input.in_transaction_id");
        this.psQueryInputsWithOutput = conn.prepareStatement(hasSpentField ? SQL_QUERY_INPUTS_WITH_OUTPUT : SQL_QUERY_INPUTS_WITH_OUTPUT.replace(",spent", ""),
                "input.transaction_id", "output.transaction_id");
        this.psQueryInputAddresses = conn.prepareStatement(SQL_QUERY_INPUT_ADDRESSES,
                "input.transaction_id", "output.transaction_id",
                "address_p2pkh.address_id", "address_p2sh.address_id", "address_p2wpkh.address_id", "address_p2wsh.address_id");
//        this.psQueryTransactionIdsAbove = conn.prepareStatement(SQL_QUERY_TRANSACTION_IDS_ABOVE);
    }

    @NonNull
    public List<TxInput> findInputsByTransactionId(int transactionId) {
        return psQueryInputs
                .setParameters(ps -> ps.setInt(transactionId))
                .executeQueryToList(
                        rs -> TxInput.builder()
                                .transactionId(transactionId)
                                .pos(rs.getShort(1))
                                .inTransactionId(rs.getInt(2))
                                .inPos(rs.getShort(3))
                                .build());
    }

    public int countInputsByTransactionId(int transactionId) {
        return DBUtils.readInteger(psCountInputsInTx.setParameters(ps -> ps.setInt(transactionId))).orElse(0);
    }

    @NonNull
    public Optional<TxInput> findInputByOutTx(int inTransactionId, short inPos) {
        return psFindInputByOutTx
                .setParameters(ps -> ps.setInt(inTransactionId).setInt(inPos))
                .querySingleRow(
                        rs -> TxInput.builder()
                                .transactionId(rs.getInt(1))
                                .pos(rs.getShort(2))
                                .inTransactionId(inTransactionId)
                                .inPos(inPos)
                                .build());
    }

    @NonNull
    public List<TxInputOutput> getInputsWithOutput(int transactionId) {
        return psQueryInputsWithOutput
                .setParameters(ps -> ps.setInt(transactionId))
                .executeQueryToList(
                        rs -> {
                            TxInputOutput.TxInputOutputBuilder builder = TxInputOutput.builder();
                            builder.input(TxInput.builder()
                                    .transactionId(transactionId)
                                    .pos(rs.getShort(1))
                                    .inTransactionId(rs.getInt(2))
                                    .inPos(rs.getShort(3))
                                    .build());
                            if (rs.getObject(4) != null) {
                                builder.output(TxOutput.builder()
                                        .transactionId(rs.getInt(2))
                                        .pos(rs.getShort(3))
                                        .addressId(rs.getInt(4))
                                        .amount(rs.getLong(5))
                                        .status(hasSpentField ? rs.getByte(6) : 0)
                                        .build());
                            }
                            return builder.build();
                        });
    }

    @NonNull
    public Collection<BtcAddress> getInputAddresses(int transactionId) {
        return psQueryInputAddresses
                .setParameters(ps -> ps.setInt(transactionId))
                .executeQueryToList(
                        rs -> BtcAddress.builder()
                                .addressId(rs.getInt(1))
                                .walletId(rs.getInt(2))
                                .build());
    }

    @NonNull
    public Set<Integer> getTransactionIdsAbove(int transactionId) {
        return DBUtils.readIntegersToSet(psQueryInputs.setParameters(ps -> ps.setInt(transactionId)));
    }

    @Getter
    @Builder
    @ToString
    public static class TxInputOutput {

        private final TxInput input;
        private final TxOutput output;
    }
}
