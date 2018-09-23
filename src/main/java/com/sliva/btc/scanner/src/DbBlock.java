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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
public class DbBlock implements SrcBlock<DbTransaction> {

    private final DbBlockProvider blockProvider;
    private int height;
    private String hash;
    private Collection<DbTransaction> transactions;

    public DbBlock(DbBlockProvider blockProvider, int height) {
        this(blockProvider, height, null);
    }

    public DbBlock(DbBlockProvider blockProvider, String hash) {
        this(blockProvider, -1, hash);
    }

    public DbBlock(DbBlockProvider blockProvider, int height, String hash) {
        this.blockProvider = blockProvider;
        this.height = height;
        this.hash = hash;
    }

    @Override
    public String getHash() {
        if (hash == null) {
            try {
                blockProvider.psQueryBlockHash.get().setInt(1, height);
                try (ResultSet rs = blockProvider.psQueryBlockHash.get().executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Block #" + height + " not found in DB");
                    }
                    hash = Hex.encodeHexString(rs.getBytes(1), true);
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return hash;
    }

    @Override
    public int getHeight() {
        if (height == -1) {
            try {
                blockProvider.psQueryBlockHeight.get().setBytes(1, Hex.decodeHex(hash));
                try (ResultSet rs = blockProvider.psQueryBlockHeight.get().executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Block " + hash + " not found in DB");
                    }
                    height = rs.getInt(1);
                }
            } catch (SQLException | DecoderException e) {
                throw new IllegalStateException(e);
            }
        }
        return height;
    }

    @Override
    public Stream<DbTransaction> getTransactions() {
        if (transactions == null) {
            try {
                blockProvider.psQueryBlockTransactions.get().setInt(1, getHeight());
                try (ResultSet rs = blockProvider.psQueryBlockTransactions.get().executeQuery()) {
                    Collection<DbTransaction> t = new ArrayList<>();
                    while (rs.next()) {
                        t.add(new DbTransaction(blockProvider, rs.getInt(1), Hex.encodeHexString(rs.getBytes(2), true)));
                    }
                    transactions = t;
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
        return transactions.stream();
    }

}
