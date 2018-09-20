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
package com.sliva.btc.scanner.tests;

import com.sliva.btc.scanner.db.DBConnection;
import com.sliva.btc.scanner.db.DBUtils;
import com.sliva.btc.scanner.db.DbQueryWallet;
import com.sliva.btc.scanner.src.DbAddress;
import com.sliva.btc.scanner.src.DbBlockProvider;
import com.sliva.btc.scanner.src.DbTransaction;
import com.sliva.btc.scanner.src.DbWallet;
import com.sliva.btc.scanner.src.SrcAddress;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author whost
 */
public class ValidateWalletConnections {

    private static final int START_WALLET_ID = 1;
    private static final String QUERY_SPENT_TRANSACTIONS_BY_ADDRESS
            = "SELECT I.transaction_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE O.address_id=?";
    private static final String QUERY_INPUT_ADDRESSES_BY_TRANSACTION_ID
            = "SELECT O.address_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE I.transaction_id=?";
    private static final DBConnection conn = new DBConnection();
    private static final ThreadLocal<PreparedStatement> psQuerySpentTransactionsByAddress = conn.prepareStatement(QUERY_SPENT_TRANSACTIONS_BY_ADDRESS);
    private static final ThreadLocal<PreparedStatement> psQueryInputAddressesByTransactionId = conn.prepareStatement(QUERY_INPUT_ADDRESSES_BY_TRANSACTION_ID);
    private static final DbBlockProvider dbBlockProvider = new DbBlockProvider(conn);
    private static final DbQueryWallet queryWallet = new DbQueryWallet(conn);

    public static void main(String[] args) throws Exception {
        final int maxWalletId = queryWallet.getMaxId();
        System.out.println("Validating wallets [" + START_WALLET_ID + " - " + maxWalletId + "]");
        for (int walletId = START_WALLET_ID; walletId <= maxWalletId; walletId++) {
            testWallet(walletId);
            if (walletId % 10000 == 0) {
                System.out.println("Last tested walletId: " + walletId);
            }
        }
    }

    private static void testWallet(int walletId) {
        DbWallet dbWallet = new DbWallet(dbBlockProvider, walletId, null, null);
//        System.out.println("Loading addresses for wallet#" + walletId + "...");
        List<SrcAddress> addresses = dbWallet.getAddresses().collect(Collectors.toList());
//        System.out.println("Addresses loaded: " + addresses.size());
        Set<SrcAddress> wholeSet = new HashSet<>(addresses);
        Set<SrcAddress> disconnectedSet = new HashSet<>(addresses);
        Set<SrcAddress> notConnectedSet = new HashSet<>();
        Set<Integer> allSpentTransactions = new HashSet<>();
//        System.out.println("Loading spent transactions for wallet#" + walletId + "...");
        addresses.forEach((a) -> {
//            System.out.println("Addr.name=" + a.getName());
            disconnectedSet.remove(a);
            allSpentTransactions.addAll(getSpentTransactions(((DbAddress) a).getAddressId()));
        });
//        System.out.println("All spent transactions (size:" + allSpentTransactions.size() + "): " + allSpentTransactions);
        Set<Integer> transactionsWithNotConnectedAddresses = new HashSet<>();
        allSpentTransactions.forEach((transactionId) -> {
            DbTransaction tx = new DbTransaction(dbBlockProvider, transactionId, null);
//            System.out.println("Transaction #" + tx.getTransactionId() + ": " + tx.getTxid());
            getInputAddresses(transactionId).forEach((addressId) -> {
                SrcAddress a = new DbAddress(dbBlockProvider, addressId, null, -1);
                if (wholeSet.contains(a)) {
                    disconnectedSet.remove(a);
//                    System.out.println("\t" + a.getName());
                } else {
                    notConnectedSet.add(a);
                    transactionsWithNotConnectedAddresses.add(transactionId);
//                    System.out.println("\t" + a.getName() + " [NOT_CONNECTED]");
                }
            });
        });
        if (!disconnectedSet.isEmpty() || !notConnectedSet.isEmpty()) {
            System.out.println("disconnectedSet(walletId:" + walletId + "): " + disconnectedSet.size());
            System.out.println("notConnectedSet(walletId:" + walletId + "): " + notConnectedSet.size() + " in " + transactionsWithNotConnectedAddresses.size() + " transactions");
        }
    }

    private static Collection<Integer> getSpentTransactions(int addressId) {
        try {
            psQuerySpentTransactionsByAddress.get().setInt(1, addressId);
            return DBUtils.readIntegers(psQuerySpentTransactionsByAddress.get());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Collection<Integer> getInputAddresses(int transactionId) {
        try {
            psQueryInputAddressesByTransactionId.get().setInt(1, transactionId);
            return DBUtils.readIntegers(psQueryInputAddressesByTransactionId.get());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
