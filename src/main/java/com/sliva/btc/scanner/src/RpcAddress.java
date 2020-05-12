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

import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.util.BJBlockHandler;
import com.sliva.btc.scanner.util.LazyInitializer;
import com.sliva.btc.scanner.util.Utils;
import lombok.ToString;
import org.bitcoinj.core.Address;

/**
 *
 * @author Sliva Co
 */
@ToString
public class RpcAddress implements SrcAddress {

    private final Address address;
    private final LazyInitializer<byte[]> hash;
    private final LazyInitializer<String> name;

    public static RpcAddress fromString(String a) {
        return new RpcAddress(BJBlockHandler.getAddress(a), a);
    }

    public RpcAddress(Address address) {
        this(address, null);
    }

    public RpcAddress(Address address, String name) {
        checkArgument(address != null, "Argument 'address' is null");
        this.address = address;
        this.hash = new LazyInitializer<>(address::getHash);
        this.name = new LazyInitializer<>(() -> name != null ? name : address.toString());
    }

    @Override
    public SrcAddressType getType() {
        return Utils.getBtcAddressType(address.getOutputScriptType());
    }

    @Override
    public byte[] getHash() {
        return hash.get();
    }

    @Override
    public String getName() {
        return name.get();
    }

}
