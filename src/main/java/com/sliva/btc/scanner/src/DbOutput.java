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

/**
 *
 * @author Sliva Co
 */
public class DbOutput implements SrcOutput<DbAddress> {

    private final DbBlockProvider blockProvider;
    private final int pos;
    private final int addressId;
    private final long amount;
    private final int spent;
    private DbAddress address;

    public DbOutput(DbBlockProvider blockProvider, int pos, int addressId, long amount, int spent) {
        this.blockProvider = blockProvider;
        this.pos = pos;
        this.addressId = addressId;
        this.amount = amount;
        this.spent = spent;
    }

    public DbBlockProvider getBlockProvider() {
        return blockProvider;
    }

    @Override
    public int getPos() {
        return pos;
    }

    @Override
    public DbAddress getAddress() {
        if (address == null) {
            address = new DbAddress(blockProvider, addressId, null, -1);
        }
        return address;
    }

    @Override
    public long getValue() {
        return amount;
    }

    public int getAddressId() {
        return addressId;
    }

    public int getSpent() {
        return spent;
    }

}
