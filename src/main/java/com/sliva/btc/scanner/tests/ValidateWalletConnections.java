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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.utils.DBUtils;
import com.sliva.btc.scanner.db.facade.DbQueryWallet;
import com.sliva.btc.scanner.db.facade.DbUpdateAddress;
import com.sliva.btc.scanner.src.DbAddress;
import com.sliva.btc.scanner.src.DbBlockProvider;
import com.sliva.btc.scanner.src.DbTransaction;
import com.sliva.btc.scanner.src.DbWallet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author whost
 */
@Slf4j
public class ValidateWalletConnections {

    private static final int START_WALLET_ID = 39_360_000;
    private static final boolean UPDATE_NOT_CONNECTED = false;
    private static final String QUERY_SPENT_TRANSACTIONS_BY_ADDRESS
            = "SELECT I.transaction_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE O.address_id=?";
    private static final String QUERY_INPUT_ADDRESSES_BY_TRANSACTION_ID
            = "SELECT O.address_id FROM input I"
            + " INNER JOIN output O ON O.transaction_id=I.in_transaction_id AND O.pos=I.in_pos"
            + " WHERE I.transaction_id=?";
    private static final DBConnectionSupplier conn = new DBConnectionSupplier();
    private static final DBPreparedStatement psQuerySpentTransactionsByAddress = conn.prepareStatement(QUERY_SPENT_TRANSACTIONS_BY_ADDRESS);
    private static final DBPreparedStatement psQueryInputAddressesByTransactionId = conn.prepareStatement(QUERY_INPUT_ADDRESSES_BY_TRANSACTION_ID);
    private static final DbBlockProvider dbBlockProvider = new DbBlockProvider(conn);
    private static final DbQueryWallet queryWallet = new DbQueryWallet(conn);
    private final DbUpdateAddress updateAddress;

    public static void main(String[] args) throws Exception {
        new ValidateWalletConnections().runProcess();
    }

    public ValidateWalletConnections() {
        if (UPDATE_NOT_CONNECTED) {
            updateAddress = new DbUpdateAddress(conn);
        } else {
            updateAddress = null;
        }
    }

    private void runProcess() throws SQLException {
        try {
            final int maxWalletId = queryWallet.getMaxId().orElse(0);
            log.info("Validating wallets [" + START_WALLET_ID + " - " + maxWalletId + "]");
            for (int walletId = START_WALLET_ID; walletId <= maxWalletId; walletId++) {
                testWallet(walletId);
                if (walletId % 10000 == 0) {
                    log.info("Last tested walletId: " + walletId);
                }
            }
        } finally {
            if (updateAddress != null) {
                updateAddress.close();
            }
        }
    }

    private void testWallet(int walletId) {
        if (walletId < 1) {
            throw new IllegalArgumentException("walletId=" + walletId);
        }
        DbWallet dbWallet = new DbWallet(dbBlockProvider, walletId, null, null);
//        log.info("Loading addresses for wallet#" + walletId + "...");
        List<DbAddress> addresses = dbWallet.getAddresses().collect(Collectors.toList());
//        log.info("Addresses loaded: " + addresses.size());
        Set<DbAddress> wholeSet = new HashSet<>(addresses);
        Set<DbAddress> disconnectedSet = new HashSet<>(addresses);
        Set<DbAddress> notConnectedSet = new HashSet<>();
        Set<Integer> allSpentTransactions = new HashSet<>();
//        log.info("Loading spent transactions for wallet#" + walletId + "...");
        addresses.forEach((a) -> {
//            log.info("Addr.name=" + a.getName());
            disconnectedSet.remove(a);
            allSpentTransactions.addAll(getSpentTransactions(a.getAddressId()));
        });
//        log.info("All spent transactions (size:" + allSpentTransactions.size() + "): " + allSpentTransactions);
        Set<Integer> transactionsWithNotConnectedAddresses = new HashSet<>();
        allSpentTransactions.forEach((transactionId) -> {
            DbTransaction tx = new DbTransaction(dbBlockProvider, transactionId, null);
            log.trace("Transaction #{}: {}", tx.getTransactionId(), tx.getTxid());
            getInputAddresses(transactionId).forEach(addressId -> {
                DbAddress a = new DbAddress(dbBlockProvider, addressId, null, -1);
                if (wholeSet.contains(a)) {
                    disconnectedSet.remove(a);
                    if (log.isTraceEnabled()) {
                        log.trace("\t{}", a.getName());
                    }
                } else {
                    notConnectedSet.add(a);
                    transactionsWithNotConnectedAddresses.add(transactionId);
                    if (log.isTraceEnabled()) {
                        log.trace("\t{} [NOT_CONNECTED]", a.getName());
                    }
                }
            });
        });
        if (!disconnectedSet.isEmpty() || !notConnectedSet.isEmpty()) {
            log.info("disconnectedSet(walletId:" + walletId + "): " + disconnectedSet.size());
            log.info("notConnectedSet(walletId:" + walletId + "): " + notConnectedSet.size() + " in " + transactionsWithNotConnectedAddresses.size() + " transactions");
            if (UPDATE_NOT_CONNECTED && !notConnectedSet.isEmpty()) {
                notConnectedSet.stream().forEach(a -> updateAddress.updateWallet(a.getAddressId(), walletId));
                //.filter(a -> a.getWalletId() == 0)
            }
        }
    }

    private Collection<Integer> getSpentTransactions(int addressId) {
        return DBUtils.readIntegersToSet(psQuerySpentTransactionsByAddress.setParameters(p -> p.setInt(addressId)));
    }

    private Collection<Integer> getInputAddresses(int transactionId) {
        return DBUtils.readIntegersToSet(psQueryInputAddressesByTransactionId.setParameters(p -> p.setInt(transactionId)));
    }
}
