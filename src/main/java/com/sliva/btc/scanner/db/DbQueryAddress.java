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
import com.sliva.btc.scanner.src.SrcAddressType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Sliva Co
 */
public class DbQueryAddress {

    private static final String SQL_FIND_BY_ADDRESS_ID = "SELECT address,wallet_id FROM address_table_name WHERE address_id=?";
    private static final String SQL_FIND_BY_ADDRESS = "SELECT address_id FROM address_table_name WHERE address=? LIMIT 1";
    private static final String SQL_QUERY_WALLET_ID = "SELECT wallet_id FROM address_table_name WHERE address_id=?";
    private static final String SQL_QUERY_LAST_ADDRESS_ID = "SELECT address_id FROM address_table_name ORDER BY address_id DESC LIMIT 1";
    private final SrcAddressType addressType;
    private final ThreadLocal<PreparedStatement> psFindByAddressId;
    private final ThreadLocal<PreparedStatement> psFindByAddress;
    private final ThreadLocal<PreparedStatement> psQueryWalletId;
    private final ThreadLocal<PreparedStatement> psQueryLastAddressId;

    public DbQueryAddress(DBConnection conn, SrcAddressType addressType) {
        this.addressType = addressType;
        this.psFindByAddressId = conn == null ? null : conn.prepareStatement(fixTableName(SQL_FIND_BY_ADDRESS_ID));
        this.psFindByAddress = conn == null ? null : conn.prepareStatement(fixTableName(SQL_FIND_BY_ADDRESS));
        this.psQueryWalletId = conn == null ? null : conn.prepareStatement(fixTableName(SQL_QUERY_WALLET_ID));
        this.psQueryLastAddressId = conn == null ? null : conn.prepareStatement(fixTableName(SQL_QUERY_LAST_ADDRESS_ID));
    }

    public String getTableName() {
        return getTableName(addressType);
    }

    public BtcAddress findByAddressId(int addressId) throws SQLException {
        psFindByAddressId.get().setInt(1, addressId);
        try (ResultSet rs = psFindByAddressId.get().executeQuery()) {
            return rs.next() ? BtcAddress.builder()
                    .type(addressType)
                    .addressId(addressId)
                    .address(rs.getBytes(1))
                    .walletId(rs.getInt(2))
                    .build() : null;
        }
    }

    public BtcAddress findByAddress(byte[] address) throws SQLException {
        psFindByAddress.get().setBytes(1, address);
        try (ResultSet rs = psFindByAddress.get().executeQuery()) {
            return rs.next() ? BtcAddress.builder()
                    .type(addressType)
                    .addressId(rs.getInt(1))
                    .address(address)
                    .build() : null;
        }
    }

    public int getWalletId(int addressId) throws SQLException {
        psQueryWalletId.get().setInt(1, addressId);
        try (ResultSet rs = psQueryWalletId.get().executeQuery()) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    public int getLastAddressId() throws SQLException {
        try (ResultSet rs = psQueryLastAddressId.get().executeQuery()) {
            return rs.next() ? rs.getInt(1)
                    : addressType == SrcAddressType.P2PKH ? BtcAddress.ADDR_P2PKH_MIN
                            : addressType == SrcAddressType.P2SH ? BtcAddress.ADDR_P2SH_MIN
                                    : addressType == SrcAddressType.P2WPKH ? BtcAddress.ADDR_P2WPKH_MIN
                                            : addressType == SrcAddressType.P2WSH ? BtcAddress.ADDR_P2WSH_MIN
                                                    : BtcAddress.ADDR_OTHER_MIN;
        }
    }

    private String fixTableName(String sql) {
        return updateQueryTableName(sql, addressType);
    }

    public static String getTableName(SrcAddressType addressType) {
        return "address_" + addressType.name().toLowerCase();
    }

    public static String updateQueryTableName(String query, SrcAddressType addressType) {
        return query.replaceAll("address_table_name", getTableName(addressType));
    }
}
