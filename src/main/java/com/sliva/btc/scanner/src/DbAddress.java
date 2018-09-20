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

import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.util.BJBlockHandler;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.EqualsAndHashCode;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
@EqualsAndHashCode(of = {"addressId"})
public class DbAddress implements SrcAddress {

    private final DbBlockProvider blockProvider;
    private final int addressId;
    private int walletId;
    private byte[] hash;
    private String name;

    public DbAddress(DbBlockProvider blockProvider, int addressId, byte[] hash, int walletId) {
        this.blockProvider = blockProvider;
        this.addressId = addressId;
        this.hash = hash;
        this.walletId = walletId;
    }

    @Override
    public SrcAddressType getType() {
        return BtcAddress.getTypeFromId(addressId);
    }

    @Override
    public byte[] getHash() {
        if (hash == null) {
            loadAddress();
        }
        return hash;
    }

    @Override
    public String getName() {
        if (name == null) {
            name = BJBlockHandler.getAddress(getType(), getHash()).toString();
        }
        return name;
    }

    public int getAddressId() {
        return addressId;
    }

    public int getWalletId() {
        if (walletId == -1) {
            loadAddress();
        }
        return walletId;
    }

    @Override
    public String toString() {
        //return "DbAddress{" + "addressId=" + getAddressId() + ", walletId=" + getWalletId() + ", name=" + getName() + '}';
        return "DbAddress{" + "addressId=" + addressId + ", walletId=" + walletId + ", name=" + name + ", hash=" + (hash == null ? null : Hex.encodeHexString(hash, true)) + '}';
    }

    private void loadAddress() {
        PreparedStatement ps = blockProvider.psQueryAddress.get(getType()).get();
        try {
            ps.setInt(1, addressId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Address #" + addressId + " not found in DB");
                }
                hash = rs.getBytes(1);
                walletId = rs.getInt(2);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
