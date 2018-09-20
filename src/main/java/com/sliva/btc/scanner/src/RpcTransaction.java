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

import com.sliva.btc.scanner.rpc.RpcClient;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.ToString;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;

/**
 *
 * @author Sliva Co
 */
@ToString
public class RpcTransaction implements SrcTransaction {

    private final String txid;
    private RawTransaction rawTransaction;

    public RpcTransaction(String txid) {
        this.txid = txid;
    }

    @Override
    public String getTxid() {
        return txid;
    }

    @Override
    public Stream<SrcInput> getInputs() {
        if (getRawTransaction().vIn() == null || getRawTransaction().vIn().isEmpty() || getRawTransaction().vIn().get(0).txid() == null) {
            return null;
        }
        final AtomicInteger pos = new AtomicInteger(0);
        return getRawTransaction().vIn().stream().map((t) -> new RpcInput(t, pos.getAndIncrement()));
    }

    @Override
    public Stream<SrcOutput> getOutputs() {
        return getRawTransaction().vOut().stream().map((t) -> new RpcOutput(t));
    }

    private RawTransaction getRawTransaction() {
        if (rawTransaction == null) {
            rawTransaction = RpcClient.getInstance().getRawTransaction(txid);
        }
        return rawTransaction;
    }
}
