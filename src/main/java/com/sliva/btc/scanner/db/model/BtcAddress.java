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
package com.sliva.btc.scanner.db.model;

import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.BJBlockHandler;
import com.sliva.btc.scanner.util.Utils;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.bitcoinj.core.Address;

/**
 *
 * @author Sliva Co
 */
@Getter
@Builder(toBuilder = true)
@ToString
@EqualsAndHashCode(of = {"addressId"})
public class BtcAddress {

    public static final int ADDR_NONE = 0;
    public static final int ADDR_P2PKH_MIN = 0x100;
    public static final int ADDR_P2PKH_MAX = 0x30000000;
    public static final int ADDR_P2SH_MIN = 0x30000001;
    public static final int ADDR_P2SH_MAX = 0x40000000;
    public static final int ADDR_P2WPKH_MIN = 0x40000001;
    public static final int ADDR_P2WPKH_MAX = 0x50000000;
    public static final int ADDR_P2WSH_MIN = 0x50000001;
    public static final int ADDR_P2WSH_MAX = 0x60000000;
    public static final int ADDR_RESERVED_MIN = 0x60000001;
    public static final int ADDR_RESERVED_MAX = 0x70000000;
    public static final int ADDR_OTHER_MIN = 0x70000001;
    public static final int ADDR_OTHER_MAX = 0x7FFFFFFF;

    private final int addressId;
    private final byte[] address;
    private final int walletId;
    private SrcAddressType type;
    private Address bjAddress;

    public SrcAddressType getType() {
        if (type == null) {
            if (addressId != 0) {
                type = getTypeFromId(addressId);
            }
            if (type == null) {
                type = SrcAddressType.UNKNOWN;
            }
        }
        return type;
    }

    public Address getBjAddress() {
        if (bjAddress == null) {
            bjAddress = BJBlockHandler.getAddress(getType(), address);
        }
        return bjAddress;
    }

    public static SrcAddressType getTypeFromId(int addressId) {
        return addressId == ADDR_NONE ? SrcAddressType.UNKNOWN
                : addressId >= ADDR_P2PKH_MIN && addressId <= ADDR_P2PKH_MAX ? SrcAddressType.P2PKH
                        : addressId >= ADDR_P2SH_MIN && addressId <= ADDR_P2SH_MAX ? SrcAddressType.P2SH
                                : addressId >= ADDR_P2WPKH_MIN && addressId <= ADDR_P2WPKH_MAX ? SrcAddressType.P2WPKH
                                        : addressId >= ADDR_P2WSH_MIN && addressId <= ADDR_P2WSH_MAX ? SrcAddressType.P2WSH
                                                : addressId >= ADDR_OTHER_MIN && addressId <= ADDR_OTHER_MAX ? SrcAddressType.OTHER
                                                        : null;
    }

    public static SrcAddressType getTypeFromAddress(String address) {
        Address a = BJBlockHandler.getAddress(address);
        if (a != null) {
            return Utils.getBtcAddressType(a.getOutputScriptType());
        }
        return null;
    }
}
