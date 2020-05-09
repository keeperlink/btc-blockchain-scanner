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

import com.sliva.btc.scanner.db.model.SighashType;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionWitness;
import org.bitcoinj.script.Script;

/**
 *
 * @author whost
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SigUtils {

    private static final byte SIG_TAG = 0x30;

    public static boolean isMultisig(TransactionInput inp) {
        return findSighash(inp).size() > 1;
    }

    public static byte getSighashType(TransactionInput inp) {
        return getSighashType(findSighash(inp));
    }

    public static byte getSighashType(List<byte[]> hashsigList) {
        for (byte[] h : hashsigList) {
            byte type = h[h.length - 1];
            if (type != SighashType.SIGHASH_ALL) {
                return type;
            }
        }
        return SighashType.SIGHASH_ALL;
    }

    public static List<byte[]> findSighash(TransactionInput inp) {
        return inp.hasWitness() ? findSighash(inp.getWitness()) : findSighash(inp.getScriptSig());
    }

    public static List<byte[]> findSighash(TransactionWitness witness) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < witness.getPushCount(); i++) {
            byte[] push = witness.getPush(i);
            if (push != null && push.length > 0 && push[0] == SIG_TAG) {
                result.add(push);
            }
        }
        return result;
    }

    @SuppressWarnings("null")
    public static List<byte[]> findSighash(Script scriptSig) {
        List<byte[]> result = new ArrayList<>();
        scriptSig.getChunks().stream()
                .filter(c -> c.isPushData() && c.data != null && c.data.length > 0 && c.data[0] == SIG_TAG)
                .forEachOrdered(c -> result.add(c.data));
        return result;
    }

}
