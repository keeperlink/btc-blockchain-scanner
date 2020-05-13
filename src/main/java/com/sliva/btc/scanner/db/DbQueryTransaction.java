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

import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.util.Utils;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;

/**
 *
 * @author Sliva Co
 */
public class DbQueryTransaction {

    private static final String SQL_QUERY_TXNS_NO_OUTPUTS = "SELECT transaction_id,txid,block_height,nInputs,nOutputs"
            + " FROM `transaction` WHERE transaction_id NOT IN (SELECT transaction_id FROM `output`) ORDER BY transaction_id";
    private static final String SQL_QUERY_TXNS_RANGE = "SELECT transaction_id,txid,block_height,nInputs,nOutputs"
            + " FROM `transaction` WHERE transaction_id BETWEEN ? AND ?";
    private static final String SQL_QUERY_TXNS_IN_BLOCK = "SELECT txid FROM `transaction` WHERE block_height=?";
    private static final String SQL_COUNT_TXNS_IN_BLOCK = "SELECT count(*) FROM `transaction` WHERE block_height=? LIMIT 1";
    private static final String SQL_FIND_TRANSACTION_BY_TXID = "SELECT transaction_id,block_height,nInputs,nOutputs FROM `transaction` WHERE txid=? LIMIT 1";
    private static final String SQL_FIND_TRANSACTION_ID_BY_TXID = "SELECT transaction_id FROM `transaction` WHERE txid=? LIMIT 1";
    private static final String SQL_FIND_TRANSACTION_BY_ID = "SELECT txid,block_height,nInputs,nOutputs FROM `transaction` WHERE transaction_id=? LIMIT 1";
    private static final String SQL_QUERY_TRANSACTIONS_IN_BLOCK = "SELECT transaction_id,txid,nInputs,nOutputs FROM `transaction` WHERE block_height=?";
    private static final String SQL_FIND_LAST_TRANSACTION = "SELECT transaction_id,txid,block_height,nInputs,nOutputs FROM `transaction` ORDER BY transaction_id DESC LIMIT 1";
    private static final String SQL_QUERY_SPENDING_TRANSACTIONS_BY_ADDRESS
            = "SELECT I.transaction_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE O.address_id=?";
    private final DBPreparedStatement psQueryTxnsNoOutputs;
    private final DBPreparedStatement psQueryTxnsRange;
    private final DBPreparedStatement psQueryTxnsInBlock;
    private final DBPreparedStatement psCountTxnsInBlock;
    private final DBPreparedStatement psFindTransactionByTxid;
    private final DBPreparedStatement psFindTransactionIdByTxid;
    private final DBPreparedStatement psFindTransactionById;
    private final DBPreparedStatement psQueryTransactionsInBlock;
    private final DBPreparedStatement psFindLastTransaction;
    private final DBPreparedStatement psQuerySpeninfTransactionsByAddress;

    public DbQueryTransaction(DBConnectionSupplier conn) {
        this.psQueryTxnsNoOutputs = conn.prepareStatement(SQL_QUERY_TXNS_NO_OUTPUTS);
        this.psQueryTxnsRange = conn.prepareStatement(SQL_QUERY_TXNS_RANGE);
        this.psQueryTxnsInBlock = conn.prepareStatement(SQL_QUERY_TXNS_IN_BLOCK);
        this.psCountTxnsInBlock = conn.prepareStatement(SQL_COUNT_TXNS_IN_BLOCK);
        this.psFindTransactionByTxid = conn.prepareStatement(SQL_FIND_TRANSACTION_BY_TXID);
        this.psFindTransactionIdByTxid = conn.prepareStatement(SQL_FIND_TRANSACTION_ID_BY_TXID);
        this.psFindTransactionById = conn.prepareStatement(SQL_FIND_TRANSACTION_BY_ID);
        this.psQueryTransactionsInBlock = conn.prepareStatement(SQL_QUERY_TRANSACTIONS_IN_BLOCK);
        this.psFindLastTransaction = conn.prepareStatement(SQL_FIND_LAST_TRANSACTION);
        this.psQuerySpeninfTransactionsByAddress = conn.prepareStatement(SQL_QUERY_SPENDING_TRANSACTIONS_BY_ADDRESS);
    }

    @NonNull
    public Optional<BtcTransaction> findTransaction(String txid) {
        byte[] binTxid = Utils.id2bin(txid);
        return psFindTransactionByTxid
                .setParameters(ps -> ps.setBytes(binTxid))
                .querySingleRow(
                        rs -> BtcTransaction.builder()
                                .transactionId(rs.getInt(1))
                                .txid(binTxid)
                                .blockHeight(rs.getInt(2))
                                .nInputs(rs.getInt(3))
                                .nOutputs(rs.getInt(4))
                                .build());
    }

    @NonNull
    public Optional<Integer> findTransactionId(String txid) {
        return DBUtils.readInteger(psFindTransactionIdByTxid.setParameters(ps -> ps.setBytes(Utils.id2bin(txid))));
    }

    @NonNull
    public Optional<BtcTransaction> findTransaction(int transactionId) {
        return psFindTransactionById
                .setParameters(ps -> ps.setInt(transactionId))
                .querySingleRow(
                        rs -> BtcTransaction.builder()
                                .transactionId(transactionId)
                                .txid(rs.getBytes(1))
                                .blockHeight(rs.getInt(2))
                                .nInputs(rs.getInt(3))
                                .nOutputs(rs.getInt(4))
                                .build());
    }

    @NonNull
    public List<BtcTransaction> getTxnsNoOutputs(int limit) {
        return psQueryTxnsNoOutputs.setMaxRows(limit).executeQueryToList(
                rs -> BtcTransaction.builder()
                        .transactionId(rs.getInt(1))
                        .txid(rs.getBytes(2))
                        .blockHeight(rs.getInt(3))
                        .nInputs(rs.getInt(4))
                        .nOutputs(rs.getInt(5))
                        .build());
    }

    @NonNull
    public List<BtcTransaction> getTxnsRangle(int startTransactionId, int endTransactionId) {
        return psQueryTxnsRange.
                setParameters(ps -> ps.setInt(startTransactionId).setInt(endTransactionId))
                .executeQueryToList(
                        rs -> BtcTransaction.builder()
                                .transactionId(rs.getInt(1))
                                .txid(rs.getBytes(2))
                                .blockHeight(rs.getInt(3))
                                .nInputs(rs.getInt(4))
                                .nOutputs(rs.getInt(5))
                                .build());
    }

    @NonNull
    public List<String> getTxnsInBlock(int blockHeight) {
        return psQueryTxnsInBlock
                .setParameters(ps -> ps.setInt(blockHeight))
                .executeQueryToList(rs -> Utils.id2hex(rs.getBytes(1)));
    }

    public int countTxnsInBlock(int blockHeight) {
        return DBUtils.readInteger(psCountTxnsInBlock.setParameters(ps -> ps.setInt(blockHeight))).orElse(0);
    }

    @NonNull
    public List<BtcTransaction> getTransactionsInBlock(int blockHeight) {
        return psQueryTransactionsInBlock
                .setParameters(ps -> ps.setInt(blockHeight))
                .executeQueryToList(
                        rs -> BtcTransaction.builder()
                                .transactionId(rs.getInt(1))
                                .txid(rs.getBytes(2))
                                .blockHeight(blockHeight)
                                .nInputs(rs.getInt(3))
                                .nOutputs(rs.getInt(4))
                                .build());
    }

    @NonNull
    public Optional<BtcTransaction> getLastTransaction() {
        return psFindLastTransaction.querySingleRow(
                rs -> BtcTransaction.builder()
                        .transactionId(rs.getInt(1))
                        .txid(rs.getBytes(2))
                        .blockHeight(rs.getInt(3))
                        .nInputs(rs.getInt(4))
                        .nOutputs(rs.getInt(5))
                        .build());
    }

    @NonNull
    public Optional<Integer> getLastTransactionId() {
        return DBUtils.readInteger(psFindLastTransaction);
    }

    @NonNull
    public Collection<Integer> getSpendingTransactionsByAddress(int addressId) {
        return DBUtils.readIntegersToSet(psQuerySpeninfTransactionsByAddress.setParameters(ps -> ps.setInt(addressId)));
    }
}
