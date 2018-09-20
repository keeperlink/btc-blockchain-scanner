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

import lombok.ToString;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.Out;

/**
 *
 * @author Sliva Co
 */
@ToString
public class RpcOutput implements SrcOutput {

    private final Out out;

    public RpcOutput(Out out) {
        this.out = out;
    }

    @Override
    public int getPos() {
        return out.n();
    }

    @Override
    public SrcAddress getAddress() {
        try {
            return BJAddress.fromString(out.scriptPubKey().addresses().get(0));
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public long getValue() {
        return Math.round(out.value() * 1e8);
    }

}
