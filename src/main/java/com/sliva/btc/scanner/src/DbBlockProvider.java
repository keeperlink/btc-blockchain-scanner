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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.model.BtcAddress;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Sliva Co
 */
public class DbBlockProvider implements BlockProvider<DbBlock> {

    private static final String SQL_QUERY_BLOCK_HASH = "SELECT hash,txn_count FROM block WHERE height=?";
    private static final String SQL_QUERY_BLOCK_HEIGHT = "SELECT height FROM block WHERE hash=?";
    private static final String SQL_QUERY_BLOCK_TRANSACTIONS = "SELECT transaction_id,txid FROM transaction WHERE block_height=?";
    private static final String SQL_QUERY_TRANSACTION_HASH = "SELECT txid FROM transaction WHERE transaction_id=?";
    private static final String SQL_QUERY_TRANSACTION_INPUTS = "SELECT I.pos,I.in_transaction_id,I.in_pos,S.sighash_type,S.segwit,S.multisig"
            + " FROM input I"
            + " LEFT JOIN input_special S ON S.transaction_id=I.transaction_id AND S.pos=I.pos"
            + " WHERE I.transaction_id=? ORDER BY I.pos";
    private static final String SQL_QUERY_TRANSACTION_OUTPUTS = "SELECT pos,address_id,amount,spent FROM output WHERE transaction_id=? ORDER BY pos";
    private static final String SQL_QUERY_ADDRESS = "SELECT address,wallet_id FROM address_table_name WHERE address_id=?";
    private static final String SQL_QUERY_WALLET = "SELECT name,details FROM wallet WHERE wallet_id=?";
    private static final String SQL_QUERY_WALLET_ADDRESSES
            = "SELECT address_id,address FROM address_p2pkh WHERE wallet_id=?"
            + " UNION SELECT address_id,address FROM address_p2sh WHERE wallet_id=?"
            + " UNION SELECT address_id,address FROM address_p2wpkh WHERE wallet_id=?"
            + " UNION SELECT address_id,CAST(address AS BINARY) FROM address_p2wsh WHERE wallet_id=?";
    final DBPreparedStatement psQueryBlockHash;
    final DBPreparedStatement psQueryBlockHeight;
    final DBPreparedStatement psQueryBlockTransactions;
    final DBPreparedStatement psQueryTransactionHash;
    final DBPreparedStatement psQueryTransactionInputs;
    final DBPreparedStatement psQueryTransactionOutputs;
    final Map<SrcAddressType, DBPreparedStatement> psQueryAddress = new HashMap<>();
    final DBPreparedStatement psQueryWallet;
    final DBPreparedStatement psQueryWalletAddresses;

    public DbBlockProvider(DBConnectionSupplier conn) {
        this.psQueryBlockHash = conn.prepareStatement(SQL_QUERY_BLOCK_HASH);
        this.psQueryBlockHeight = conn.prepareStatement(SQL_QUERY_BLOCK_HEIGHT);
        this.psQueryBlockTransactions = conn.prepareStatement(SQL_QUERY_BLOCK_TRANSACTIONS);
        this.psQueryTransactionHash = conn.prepareStatement(SQL_QUERY_TRANSACTION_HASH);
        this.psQueryTransactionInputs = conn.prepareStatement(SQL_QUERY_TRANSACTION_INPUTS);
        this.psQueryTransactionOutputs = conn.prepareStatement(SQL_QUERY_TRANSACTION_OUTPUTS);
        BtcAddress.getRealTypes().forEach((type) -> psQueryAddress.put(type, conn.prepareStatement(fixTableName(SQL_QUERY_ADDRESS, type))));
        this.psQueryWallet = conn.prepareStatement(SQL_QUERY_WALLET);
        this.psQueryWalletAddresses = conn.prepareStatement(SQL_QUERY_WALLET_ADDRESSES);
    }

    @Override
    public DbBlock getBlock(int height) {
        return new DbBlock(this, height);
    }

    @Override
    public DbBlock getBlock(String hash) {
        return new DbBlock(this, hash);
    }

    private String fixTableName(String sql, SrcAddressType type) {
        return sql.replaceAll("address_table_name", "address_" + type.name().toLowerCase());
    }
}
