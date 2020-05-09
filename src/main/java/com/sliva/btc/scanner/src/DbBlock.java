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

import static com.sliva.btc.scanner.util.Utils.decodeHex;
import static com.sliva.btc.scanner.util.Utils.encodeHex;
import java.util.Collection;
import java.util.stream.Stream;
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
            hash = blockProvider.psQueryBlockHash
                    .setParameters(p -> p.setInt(height))
                    .querySingleRow(rs -> Hex.encodeHexString(rs.getBytes(1), true))
                    .orElseThrow(() -> new IllegalStateException("Block #" + height + " not found in DB"));
        }
        return hash;
    }

    @Override
    public int getHeight() {
        if (height == -1) {
            height = blockProvider.psQueryBlockHeight
                    .setParameters(p -> p.setBytes(decodeHex(hash)))
                    .querySingleRow(rs -> rs.getInt(1))
                    .orElseThrow(() -> new IllegalStateException("Block " + hash + " not found in DB"));
        }
        return height;
    }

    @Override
    public Stream<DbTransaction> getTransactions() {
        if (transactions == null) {
            transactions = blockProvider.psQueryBlockTransactions
                    .setParameters(p -> p.setInt(getHeight()))
                    .executeQueryToList(rs -> new DbTransaction(blockProvider, rs.getInt(1), encodeHex(rs.getBytes(2))));
        }
        return transactions.stream();
    }
}
