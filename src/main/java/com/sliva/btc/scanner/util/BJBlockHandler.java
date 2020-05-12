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
package com.sliva.btc.scanner.util;

import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.src.SrcAddressType;
import java.io.File;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.io.FileUtils;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.BitcoinSerializer;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.LegacyAddress;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.SegwitAddress;
import org.bitcoinj.params.MainNetParams;

/**
 *
 * @author Sliva Co
 */
@SuppressWarnings("ResultOfObjectAllocationIgnored")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BJBlockHandler {

    public static File FULL_BLOCKS_PATH = new File("/CryptoData/btc-scanner/full_blocks");
    private static final ThreadLocal<NetworkParameters> np = ThreadLocal.withInitial(() -> {
        NetworkParameters params = new MainNetParams();
        new Context(params);
        return params;
    });
    private static final ThreadLocal<BitcoinSerializer> bitcoinSerializer = ThreadLocal.withInitial(() -> new BitcoinSerializer(np.get(), false));

    @NonNull
    public static NetworkParameters getNetworkParams() {
        return np.get();
    }

    @NonNull
    public static Block getBlock(String blockHash) throws IOException {
        File f = new File(FULL_BLOCKS_PATH, blockHash + ".block");
        if (!f.exists()) {
            throw new IllegalArgumentException("File not found: " + f.getAbsolutePath());
        }
        try {
            return parseBlcok(FileUtils.readFileToByteArray(f));
        } catch (org.bitcoinj.core.ProtocolException e) {
            throw new IOException("blockHash=" + blockHash, e);
        }
    }

    @NonNull
    public static Block parseBlcok(byte[] rawBlockData) {
        return bitcoinSerializer.get().makeBlock(rawBlockData);
    }

    @NonNull
    public static Address getAddress(String address) {
        return Address.fromString(np.get(), address);
    }

    @NonNull
    public static Address getAddress(SrcAddressType addressType, byte[] hash) {
        checkArgument(addressType != null, "Argument 'addressType' is null");
        checkArgument(hash != null, "Argument 'hash' is null");
        checkArgument(addressType.isReal(), "Argument 'addressType' is not a real address type: %s", addressType);
        switch (addressType) {
            case P2PKH:
                return LegacyAddress.fromPubKeyHash(np.get(), hash);
            case P2SH:
                return LegacyAddress.fromScriptHash(np.get(), hash);
            case P2WPKH:
            case P2WSH:
                return SegwitAddress.fromHash(np.get(), hash);
            default:
                throw new IllegalArgumentException("Unknown address type: " + addressType);
        }
    }
}
