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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;

/**
 *
 * @author Sliva Co
 */
public class DbQueryAddressOne {

    private static final String ADDRESS_TABLE_NAME = "address_table_name";
    private static final String SQL_FIND_BY_ADDRESS_ID = "SELECT `address`,wallet_id FROM `address_table_name` WHERE address_id=? LIMIT 1";
    private static final String SQL_FIND_BY_ADDRESS = "SELECT address_id FROM `address_table_name` WHERE `address`=? LIMIT 1";
    private static final String SQL_QUERY_WALLET_ID = "SELECT wallet_id FROM `address_table_name` WHERE address_id=? LIMIT 1";
    private static final String SQL_QUERY_LAST_ADDRESS_ID = "SELECT address_id FROM `address_table_name` ORDER BY address_id DESC LIMIT 1";
    private final SrcAddressType addressType;
    private final DBPreparedStatement psFindByAddressId;
    private final DBPreparedStatement psFindByAddress;
    private final DBPreparedStatement psQueryWalletId;
    private final DBPreparedStatement psQueryLastAddressId;
    @Getter
    private final String tableName;

    public DbQueryAddressOne() {
        this.addressType = null;
        this.psFindByAddressId = null;
        this.psFindByAddress = null;
        this.psQueryWalletId = null;
        this.psQueryLastAddressId = null;
        this.tableName = null;
    }

    public DbQueryAddressOne(DBConnectionSupplier conn, SrcAddressType addressType) {
        checkArgument(conn != null, "Argument 'conn' is null");
        checkArgument(addressType != null, "Argument 'addressType' is null");
        this.addressType = addressType;
        this.tableName = getAddressTableName(addressType);
        this.psFindByAddressId = conn == null ? null : conn.prepareStatement(fixTableName(SQL_FIND_BY_ADDRESS_ID), tableName + ".address_id");
        this.psFindByAddress = conn == null ? null : conn.prepareStatement(fixTableName(SQL_FIND_BY_ADDRESS), tableName + ".address");
        this.psQueryWalletId = conn == null ? null : conn.prepareStatement(fixTableName(SQL_QUERY_WALLET_ID), tableName + ".address_id");
        this.psQueryLastAddressId = conn == null ? null : conn.prepareStatement(fixTableName(SQL_QUERY_LAST_ADDRESS_ID), tableName + ".address_id");
    }

    @NonNull
    public Optional<BtcAddress> findByAddressId(int addressId) {
        checkState(psFindByAddressId != null, "Method not supported due to instance created with no-arguments constructor");
        return psFindByAddressId.setParameters(ps -> ps.setInt(addressId)).querySingleRow(rs -> BtcAddress.builder()
                .type(addressType)
                .addressId(addressId)
                .address(rs.getBytes(1))
                .walletId(rs.getInt(2))
                .build());
    }

    @NonNull
    public Optional<BtcAddress> findByAddress(byte[] address) {
        checkState(addressType != null, "Method not supported due to instance created with no-arguments constructor");
        return DBUtils.readInteger(psFindByAddress.setParameters(ps -> ps.setBytes(address)))
                .map(addressId -> BtcAddress.builder()
                .type(addressType)
                .addressId(addressId)
                .address(address)
                .build());
    }

    @NonNull
    public Optional<Integer> getWalletId(int addressId) {
        checkState(addressType != null, "Method not supported due to instance created with no-arguments constructor");
        return DBUtils.readInteger(psQueryWalletId.setParameters(p -> p.setInt(addressId)));
    }

    public int getLastAddressId() {
        checkState(addressType != null, "Method not supported due to instance created with no-arguments constructor");
        return DBUtils.readInteger(psQueryLastAddressId).orElseGet(()
                -> addressType == SrcAddressType.P2PKH ? BtcAddress.ADDR_P2PKH_MIN
                        : addressType == SrcAddressType.P2SH ? BtcAddress.ADDR_P2SH_MIN
                                : addressType == SrcAddressType.P2WPKH ? BtcAddress.ADDR_P2WPKH_MIN
                                        : addressType == SrcAddressType.P2WSH ? BtcAddress.ADDR_P2WSH_MIN
                                                : BtcAddress.ADDR_OTHER_MIN);
    }

    @NonNull
    private String fixTableName(String query) {
        return query.replaceAll(ADDRESS_TABLE_NAME, getTableName());
    }

    @NonNull
    public static String getAddressTableName(SrcAddressType addressType) {
        return "address_" + addressType.name().toLowerCase();
    }

    @NonNull
    public static String updateQueryTableName(String query, SrcAddressType addressType) {
        return query.replaceAll(ADDRESS_TABLE_NAME, getAddressTableName(addressType));
    }
}
