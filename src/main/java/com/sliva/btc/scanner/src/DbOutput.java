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

import static com.sliva.btc.scanner.db.model.BtcAddress.ADDR_NONE;
import com.sliva.btc.scanner.util.LazyInitializer;
import java.util.Optional;
import lombok.Getter;

/**
 *
 * @author Sliva Co
 */
public class DbOutput implements SrcOutput<DbAddress> {

    @Getter
    private final DbBlockProvider blockProvider;
    @Getter
    private final short pos;
    @Getter
    private final int addressId;
    @Getter
    private final long value;
    @Getter
    private final byte spent;
    private final LazyInitializer<Optional<DbAddress>> address;

    public DbOutput(DbBlockProvider blockProvider, short pos, int addressId, long value, byte spent) {
        this.blockProvider = blockProvider;
        this.pos = pos;
        this.addressId = addressId;
        this.value = value;
        this.spent = spent;
        this.address = new LazyInitializer<>(() -> addressId == ADDR_NONE ? Optional.empty() : Optional.of(new DbAddress(blockProvider, addressId, null, -1)));
    }

    @Override
    public Optional<DbAddress> getAddress() {
        return address.get();
    }
}
