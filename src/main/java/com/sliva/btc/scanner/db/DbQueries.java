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

import com.sliva.btc.scanner.db.model.BtcAddress;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author whost
 */
public class DbQueries {

    private final DbQueryInput queryInput;
    private final DbQueryTransaction queryTransaction;

    public DbQueries(DBConnection con) {
        this.queryInput = new DbQueryInput(con);
        this.queryTransaction = new DbQueryTransaction(con);
    }

    public Collection<BtcAddress> getRelatedAddresses(int transactionId) throws SQLException {
        return new RelatedAddressesProc().processTransaction(transactionId).result;
    }

    private class RelatedAddressesProc {

        final Collection<BtcAddress> result = new HashSet<>();
        final Set<Integer> processedTxn = new HashSet<>();

        RelatedAddressesProc processTransaction(int transactionId) {
            if (!processedTxn.contains(transactionId)) {
                processedTxn.add(transactionId);
                try {
                    queryInput.getInputAddresses(transactionId).forEach(a -> processAddress(a));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return this;
        }

        RelatedAddressesProc processAddress(BtcAddress address) {
            if (!result.contains(address)) {
                result.add(address);
                try {
                    queryTransaction.getSpendingTransactionsByAddress(address.getAddressId()).forEach(t -> processTransaction(t));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return this;
        }
    }
}
