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
import com.sliva.btc.scanner.util.LazyInitializer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.ECKey;
import org.bitcoinj.core.LegacyAddress;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.script.ScriptOpCodes;

/**
 *
 * @author Sliva Co
 * @param <A>
 */
@ToString
public class BJOutput<A extends BJAddress> implements SrcOutput<BJAddress> {

    @Getter
    private final short pos;
    @Getter
    private final long value;
    private final LazyInitializer<Optional<BJAddress>> bjAddress;

    public BJOutput(TransactionOutput to) {
        this.pos = (short) to.getIndex();
        this.value = to.getValue().longValue();
        bjAddress = new LazyInitializer<>(() -> _getAddress(to));
    }

    @NonNull
    @Override
    public Optional<BJAddress> getAddress() {
        return bjAddress.get();
    }

    @NonNull
    @SuppressWarnings("UseSpecificCatch")
    private Optional<BJAddress> _getAddress(TransactionOutput to) {
        Address adr;
        try {
            adr = to.getScriptPubKey().getToAddress(BJBlockHandler.getNetworkParams(), true);
        } catch (Exception e) {
            try {
                if (noAddressScriptOpCodes.contains(to.getScriptPubKey().getChunks().get(0).opcode)) {
                    return Optional.empty();
                } else {
                    adr = LegacyAddress.fromKey(BJBlockHandler.getNetworkParams(), ECKey.fromPublicOnly(to.getScriptPubKey().getChunks().get(1).data));
                }
            } catch (Exception e2) {
                return Optional.empty();
            }
        }
        return Optional.of(new BJAddress(adr));
    }
    private static final Set<Integer> noAddressScriptOpCodes = new HashSet<>(Arrays.asList(ScriptOpCodes.OP_RETURN, ScriptOpCodes.OP_DUP));
}
