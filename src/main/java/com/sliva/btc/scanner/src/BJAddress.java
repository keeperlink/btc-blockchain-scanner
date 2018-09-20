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

import com.sliva.btc.scanner.util.BJBlockHandler;
import com.sliva.btc.scanner.util.Utils;
import lombok.ToString;
import org.bitcoinj.core.Address;

/**
 *
 * @author Sliva Co
 */
@ToString
public class BJAddress implements SrcAddress {

    private final Address address;

    public static BJAddress fromString(String a) {
        return new BJAddress(BJBlockHandler.getAddress(a));
    }

    public BJAddress(Address address) {
        this.address = address;
    }

    @Override
    public SrcAddressType getType() {
        return Utils.getBtcAddressType(address.getOutputScriptType());
    }

    @Override
    public byte[] getHash() {
        return address.getHash();
    }

    @Override
    public String getName() {
        return address.toString();
    }

}
