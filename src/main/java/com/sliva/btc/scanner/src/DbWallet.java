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

import java.util.Collection;
import java.util.stream.Stream;

/**
 *
 * @author Sliva Co
 */
public class DbWallet implements SrcWallet<DbAddress> {

    private final DbBlockProvider blockProvider;
    private final int id;
    private String name;
    private String details;
    private boolean loaded;
    private Collection<DbAddress> addresses;

    public DbWallet(DbBlockProvider blockProvider, int id, String name, String details) {
        this.blockProvider = blockProvider;
        this.id = id;
        this.name = name;
        this.details = details;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        if (name == null && !loaded) {
            loadWallet();
        }
        return name;
    }

    @Override
    public String getDetails() {
        if (details == null && !loaded) {
            loadWallet();
        }
        return details;
    }

    @Override
    public Stream<DbAddress> getAddresses() {
        if (addresses == null) {
            addresses = blockProvider.psQueryWalletAddresses
                    .setParameters(p -> p.setInt(id).setInt(id).setInt(id).setInt(id))
                    .executeQueryToList(rs -> new DbAddress(blockProvider, rs.getInt(1), rs.getBytes(2), id));
        }
        return addresses.stream();
    }

    private void loadWallet() {
        blockProvider.psQueryWallet.setParameters(p -> p.setInt(id)).querySingleRow(rs -> {
            name = rs.getString(1);
            details = rs.getString(2);
            return true;
        }).orElseThrow(() -> new IllegalStateException("Wallet #" + id + " not found in DB"));
        loaded = true;
    }
}
