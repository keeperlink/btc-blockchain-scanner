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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Sliva Co
 */
public class DbQueryTransaction {

    private static final String SQL_QUERY_TXNS_NO_OUTPUTS = "SELECT transaction_id,txid,block_height,nInputs,nOutputs FROM transaction WHERE transaction_id NOT IN (SELECT transaction_id FROM output) ORDER BY transaction_id";
    private static final String SQL_QUERY_TXNS_RANGE = "SELECT transaction_id,txid,block_height,nInputs,nOutputs FROM transaction WHERE transaction_id BETWEEN ? AND ?";
    private static final String SQL_QUERY_TXNS_IN_BLOCK = "SELECT txid FROM transaction WHERE block_height=?";
    private static final String SQL_COUNT_TXNS_IN_BLOCK = "SELECT count(*) FROM transaction WHERE block_height=?";
    private static final String SQL_FIND_TRANSACTION_BY_TXID = "SELECT transaction_id,block_height,nInputs,nOutputs FROM transaction WHERE txid=?";
    private static final String SQL_FIND_TRANSACTION_ID_BY_TXID = "SELECT transaction_id FROM transaction WHERE txid=?";
    private static final String SQL_FIND_TRANSACTION_BY_ID = "SELECT txid,block_height,nInputs,nOutputs FROM transaction WHERE transaction_id=?";
    private static final String SQL_QUERY_TRANSACTIONS_IN_BLOCK = "SELECT transaction_id,txid,nInputs,nOutputs FROM transaction WHERE block_height=?";
    private static final String SQL_FIND_LAST_TRANSACTION = "SELECT transaction_id,txid,block_height,nInputs,nOutputs FROM transaction ORDER BY transaction_id DESC LIMIT 1";
    private static final String SQL_QUERY_SPENDING_TRANSACTIONS_BY_ADDRESS
            = "SELECT I.transaction_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE O.address_id=?";
    private final ThreadLocal<PreparedStatement> psQueryTxnsNoOutputs;
    private final ThreadLocal<PreparedStatement> psQueryTxnsRange;
    private final ThreadLocal<PreparedStatement> psQueryTxnsInBlock;
    private final ThreadLocal<PreparedStatement> psCountTxnsInBlock;
    private final ThreadLocal<PreparedStatement> psFindTransactionByTxid;
    private final ThreadLocal<PreparedStatement> psFindTransactionIdByTxid;
    private final ThreadLocal<PreparedStatement> psFindTransactionById;
    private final ThreadLocal<PreparedStatement> psQueryTransactionsInBlock;
    private final ThreadLocal<PreparedStatement> psFindLastTransaction;
    private final ThreadLocal<PreparedStatement> psQuerySpeninfTransactionsByAddress;

    public DbQueryTransaction(DBConnection conn) {
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

    public BtcTransaction findTransaction(String txid) throws SQLException {
        psFindTransactionByTxid.get().setBytes(1, Utils.id2bin(txid));
        try (ResultSet rs = psFindTransactionByTxid.get().executeQuery()) {
            return rs.next() ? BtcTransaction.builder()
                    .transactionId(rs.getInt(1))
                    .txid(txid)
                    .blockHeight(rs.getInt(2))
                    .nInputs(rs.getInt(3))
                    .nOutputs(rs.getInt(4))
                    .build() : null;
        }
    }

    public int findTransactionId(String txid) throws SQLException {
        psFindTransactionIdByTxid.get().setBytes(1, Utils.id2bin(txid));
        try (ResultSet rs = psFindTransactionIdByTxid.get().executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public BtcTransaction findTransaction(int transactionId) throws SQLException {
        psFindTransactionById.get().setInt(1, transactionId);
        try (ResultSet rs = psFindTransactionById.get().executeQuery()) {
            return rs.next() ? BtcTransaction.builder()
                    .transactionId(transactionId)
                    .txid(Utils.id2hex(rs.getBytes(1)))
                    .blockHeight(rs.getInt(2))
                    .nInputs(rs.getInt(3))
                    .nOutputs(rs.getInt(4))
                    .build() : null;
        }
    }

    public List<BtcTransaction> getTxnsNoOutputs(int limit) throws SQLException {
        psQueryTxnsNoOutputs.get().setMaxRows(limit);
        try (ResultSet rs = psQueryTxnsNoOutputs.get().executeQuery()) {
            List<BtcTransaction> result = null;
            while (rs.next()) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(BtcTransaction.builder()
                        .transactionId(rs.getInt(1))
                        .txid(Utils.id2hex(rs.getBytes(2)))
                        .blockHeight(rs.getInt(3))
                        .nInputs(rs.getInt(4))
                        .nOutputs(rs.getInt(5))
                        .build());
            }
            return result;
        }
    }

    public List<BtcTransaction> getTxnsRangle(int startTransactionId, int endTransactionId) throws SQLException {
        psQueryTxnsRange.get().setInt(1, startTransactionId);
        psQueryTxnsRange.get().setInt(2, endTransactionId);
        try (ResultSet rs = psQueryTxnsRange.get().executeQuery()) {
            List<BtcTransaction> result = null;
            while (rs.next()) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(BtcTransaction.builder()
                        .transactionId(rs.getInt(1))
                        .txid(Utils.id2hex(rs.getBytes(2)))
                        .blockHeight(rs.getInt(3))
                        .nInputs(rs.getInt(4))
                        .nOutputs(rs.getInt(5))
                        .build());
            }
            return result;
        }
    }

    public List<String> getTxnsInBlock(int blockHeight) throws SQLException {
        psQueryTxnsInBlock.get().setInt(1, blockHeight);
        try (ResultSet rs = psQueryTxnsInBlock.get().executeQuery()) {
            List<String> result = null;
            while (rs.next()) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(Utils.id2hex(rs.getBytes(1)));
            }
            return result;
        }
    }

    public int countTxnsInBlock(int blockHeight) throws SQLException {
        psCountTxnsInBlock.get().setInt(1, blockHeight);
        try (ResultSet rs = psCountTxnsInBlock.get().executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public List<BtcTransaction> getTransactionsInBlock(int blockHeight) throws SQLException {
        psQueryTransactionsInBlock.get().setInt(1, blockHeight);
        try (ResultSet rs = psQueryTransactionsInBlock.get().executeQuery()) {
            List<BtcTransaction> result = null;
            while (rs.next()) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(BtcTransaction.builder()
                        .transactionId(rs.getInt(1))
                        .txid(Utils.id2hex(rs.getBytes(2)))
                        .blockHeight(blockHeight)
                        .nInputs(rs.getInt(3))
                        .nOutputs(rs.getInt(4))
                        .build());
            }
            return result;
        }
    }

    public BtcTransaction getLastTransaction() throws SQLException {
        try (ResultSet rs = psFindLastTransaction.get().executeQuery()) {
            return rs.next() ? BtcTransaction.builder()
                    .transactionId(rs.getInt(1))
                    .txid(Utils.id2hex(rs.getBytes(2)))
                    .blockHeight(rs.getInt(3))
                    .nInputs(rs.getInt(4))
                    .nOutputs(rs.getInt(5))
                    .build() : null;
        }
    }

    public int getLastTransactionId() throws SQLException {
        try (ResultSet rs = psFindLastTransaction.get().executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public Collection<Integer> getSpendingTransactionsByAddress(int addressId) throws SQLException {
        psQuerySpeninfTransactionsByAddress.get().setInt(1, addressId);
        return DBUtils.readIntegersToSet(psQuerySpeninfTransactionsByAddress.get());
    }
}
