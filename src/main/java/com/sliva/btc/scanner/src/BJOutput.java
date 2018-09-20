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
import lombok.ToString;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.ECKey;
import org.bitcoinj.core.LegacyAddress;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.script.ScriptOpCodes;

/**
 *
 * @author Sliva Co
 */
@ToString
public class BJOutput implements SrcOutput {

    private final TransactionOutput to;

    public BJOutput(TransactionOutput to) {
        this.to = to;
    }

    @Override
    public int getPos() {
        return to.getIndex();
    }

    @Override
    @SuppressWarnings("UseSpecificCatch")
    public SrcAddress getAddress() {
        Address adr;
        try {
            adr = to.getScriptPubKey().getToAddress(BJBlockHandler.getNetworkParams(), true);
        } catch (Exception e) {
            try {
                if (to.getScriptPubKey().getChunks().get(0).opcode == ScriptOpCodes.OP_RETURN) {
                    adr = null;
                } else {
                    adr = LegacyAddress.fromKey(BJBlockHandler.getNetworkParams(), ECKey.fromPublicOnly(to.getScriptPubKey().getChunks().get(1).data));
                }
            } catch (Exception e2) {
                adr = null;
            }
        }
        return adr == null ? null : new BJAddress(adr);
    }

    @Override
    public long getValue() {
        return to.getValue().longValue();
    }
}
