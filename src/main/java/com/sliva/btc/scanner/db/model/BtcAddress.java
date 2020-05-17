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
import com.sliva.btc.scanner.util.LazyInitializer;
import com.sliva.btc.scanner.util.Utils;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.bitcoinj.core.Address;

/**
 *
 * @author Sliva Co
 */
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

    @Getter
    private final int addressId;
    @NonNull
    private final byte[] address;
    @Getter
    private final int walletId;
    @Getter
    @NonNull
    private final SrcAddressType type;
    private final LazyInitializer<Optional<Address>> bjAddress;

    @Builder(toBuilder = true)
    public BtcAddress(int addressId, byte[] address, int walletId, SrcAddressType type) {
        this.addressId = addressId;
        this.address = address;
        this.walletId = walletId;
        this.type = type != null && type.isReal() ? type : getTypeFromId(addressId);
        this.bjAddress = new LazyInitializer<>(() -> this.type.isReal() ? Optional.of(BJBlockHandler.getAddress(this.type, address)) : Optional.empty());
    }

    public BinaryAddress getAddress() {
        return new BinaryAddress(address);
    }

    @NonNull
    public Optional<Address> getBjAddress() {
        return bjAddress.get();
    }

    public static boolean isRealAddress(int addressId) {
        return addressId >= ADDR_P2PKH_MIN && addressId <= ADDR_P2WSH_MAX;
    }

    @NonNull
    public static SrcAddressType getTypeFromId(int addressId) {
        return addressId == ADDR_NONE ? SrcAddressType.UNKNOWN
                : addressId >= ADDR_P2PKH_MIN && addressId <= ADDR_P2PKH_MAX ? SrcAddressType.P2PKH
                        : addressId >= ADDR_P2SH_MIN && addressId <= ADDR_P2SH_MAX ? SrcAddressType.P2SH
                                : addressId >= ADDR_P2WPKH_MIN && addressId <= ADDR_P2WPKH_MAX ? SrcAddressType.P2WPKH
                                        : addressId >= ADDR_P2WSH_MIN && addressId <= ADDR_P2WSH_MAX ? SrcAddressType.P2WSH
                                                : addressId >= ADDR_OTHER_MIN && addressId <= ADDR_OTHER_MAX ? SrcAddressType.OTHER
                                                        : SrcAddressType.UNKNOWN;
    }

    @Nullable
    public static SrcAddressType getTypeFromAddress(String address) {
        Address a = BJBlockHandler.getAddress(address);
        return Utils.getBtcAddressType(a.getOutputScriptType());
    }
}
