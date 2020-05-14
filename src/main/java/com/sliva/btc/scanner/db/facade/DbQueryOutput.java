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
import com.sliva.btc.scanner.db.utils.DBUtils;
import static com.google.common.base.Preconditions.checkArgument;
import static com.sliva.btc.scanner.db.facade.DbQueryAddressOne.updateQueryTableName;
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxInput;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.src.SrcAddressType;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import static com.sliva.btc.scanner.db.facade.DbQueryAddressOne.getAddressTableName;

/**
 *
 * @author Sliva Co
 */
public class DbQueryOutput {

    private static final int MAX_OUTS_IN_TXN = 999999;
    private static final String SQL_QUERY_OUTPUTS = "SELECT pos,address_id,amount,spent"
            + " FROM `output` WHERE transaction_id=? LIMIT " + MAX_OUTS_IN_TXN;
    private static final String SQL_COUNT_OUTPUTS_IN_TX = "SELECT count(*) FROM `output` WHERE transaction_id=? LIMIT 1";
    private static final String SQL_QUERY_OUTPUT = "SELECT address_id,amount,spent"
            + " FROM `output` WHERE transaction_id=? AND pos=? LIMIT 1";
    private static final String SQL_QUERY_OUTPUTS_WITH_INPUT = "SELECT O.pos,O.address_id,O.amount,O.spent"
            + ",I.transaction_id,I.pos"
            + " FROM `output` O"
            + " LEFT JOIN `input` I ON I.in_transaction_id=O.transaction_id AND I.in_pos=O.pos"
            + " WHERE O.transaction_id=? LIMIT " + MAX_OUTS_IN_TXN;
    private static final String SQL_QUERY_OUTPUTS_IN_TXN_RANGE = "SELECT O.transaction_id,O.pos"
            + ",O.address_id,O.amount,O.spent,A.wallet_id,W.name"
            + " FROM `output` O"
            + " INNER JOIN address_table_name A ON A.address_id=O.address_id"
            + " INNER JOIN wallet W ON W.wallet_id=A.wallet_id"
            + " WHERE transaction_id BETWEEN ? AND ?";
    private final DBPreparedStatement psQueryOutputs;
    private final DBPreparedStatement psCountOutputsInTx;
    private final DBPreparedStatement psQueryOutput;
    private final DBPreparedStatement psQueryOutputsWithInput;
    private final Map<SrcAddressType, DBPreparedStatement> psQueryOutputsInTxnRange = new HashMap<>();

    public DbQueryOutput(DBConnectionSupplier conn) {
        this.psQueryOutputs = conn.prepareStatement(SQL_QUERY_OUTPUTS, "output.transaction_id");
        this.psCountOutputsInTx = conn.prepareStatement(SQL_COUNT_OUTPUTS_IN_TX, "output.transaction_id");
        this.psQueryOutput = conn.prepareStatement(SQL_QUERY_OUTPUT, "output.transaction_id");
        this.psQueryOutputsWithInput = conn.prepareStatement(SQL_QUERY_OUTPUTS_WITH_INPUT, "output.transaction_id", "input.transaction_id");
        Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).forEach(t -> psQueryOutputsInTxnRange.put(t,
                conn.prepareStatement(updateQueryTableName(SQL_QUERY_OUTPUTS_IN_TXN_RANGE, t), "output.transaction_id", getAddressTableName(t) + ".address_id")));
    }

    @NonNull
    public List<TxOutput> findOutputsByTransactionId(int transactionId) {
        return psQueryOutputs.setParameters(ps -> ps.setInt(transactionId)).executeQueryToList(rs
                -> TxOutput.builder()
                        .transactionId(transactionId)
                        .pos(rs.getShort(1))
                        .addressId(rs.getInt(2))
                        .amount(rs.getLong(3))
                        .status(rs.getByte(4))
                        .build());
    }

    public int countOutputsByTransactionId(int transactionId) {
        return DBUtils.readInteger(psCountOutputsInTx.setParameters(ps -> ps.setInt(transactionId))).orElse(0);
    }

    @NonNull
    public Optional<TxOutput> getOutput(int transactionId, short pos) {
        return psQueryOutput.setParameters(ps -> ps.setInt(transactionId).setShort(pos)).querySingleRow(rs -> TxOutput.builder()
                .transactionId(transactionId)
                .pos(pos)
                .addressId(rs.getInt(1))
                .amount(rs.getLong(2))
                .status(rs.getByte(3))
                .build());
    }

    @NonNull
    public List<TxOutputInput> getOutputsWithInput(int transactionId) {
        return psQueryOutputsWithInput.setParameters(ps -> ps.setInt(transactionId)).executeQueryToList(rs -> {
            TxOutputInput.TxOutputInputBuilder builder = TxOutputInput.builder();
            builder.output(TxOutput.builder()
                    .transactionId(transactionId)
                    .pos(rs.getShort(1))
                    .addressId(rs.getInt(2))
                    .amount(rs.getLong(3))
                    .status(rs.getByte(4))
                    .build());
            if (rs.getObject(5) != null) {
                builder.input(TxInput.builder()
                        .transactionId(rs.getInt(5))
                        .pos(rs.getShort(6))
                        .inTransactionId(transactionId)
                        .inPos(rs.getShort(1))
                        .build());
            }
            return builder.build();
        });
    }

    @NonNull
    public Collection<OutputAddressWallet> queryOutputsInTxnRange(int startTxId, int endTxId, SrcAddressType addressType) {
        checkArgument(addressType.isReal(), "Argument addressType=%s, allowed only real types.", addressType);
        return psQueryOutputsInTxnRange.get(addressType).setParameters(p -> p.setInt(startTxId).setInt(endTxId))
                .executeQueryToList(rs
                        -> OutputAddressWallet.builder()
                        .transactionId(rs.getInt(1))
                        .pos(rs.getShort(2))
                        .addressId(rs.getInt(3))
                        .amount(rs.getLong(4))
                        .status(rs.getByte(5))
                        .walletId(rs.getInt(6))
                        .walletName(rs.getString(7))
                        .build());
    }

    @Getter
    @Builder
    @ToString
    public static class TxOutputInput {

        private final TxOutput output;
        private final TxInput input;
    }

    @Getter
    @SuperBuilder(toBuilder = true)
    @ToString(callSuper = true)
    public static class OutputAddressWallet extends InOutKey {

        private final int addressId;
        private final long amount;
        private final byte status;
        private final int walletId;
        private final String walletName;
    }
}
