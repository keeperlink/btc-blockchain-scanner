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
import com.sliva.btc.scanner.util.LazyInitializer;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.bitcoinj.core.TransactionOutput;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.Out;

/**
 *
 * @author Sliva Co
 * @param <A>
 */
@ToString
public class RpcOutput<A extends RpcAddress> implements SrcOutput<RpcAddress> {

    @Getter
    private final short pos;
    @Getter
    private final long value;
    private final LazyInitializer<Optional<RpcAddress>> rpcAddress;

    public RpcOutput(Out out) {
        checkArgument(out != null, "Argument 'out' is null");
        pos = (short) out.n();
        value = out.value().movePointRight(8).longValueExact();
        rpcAddress = new LazyInitializer<>(() -> Optional.of(out.scriptPubKey().addresses()).map(a -> a.size() > 0 ? a.get(0) : null).map(RpcAddress::fromString));
    }

    public RpcOutput(TransactionOutput txout) {
        checkArgument(txout != null, "Argument 'txout' is null");
        pos = (short) txout.getIndex();
        value = txout.getValue().getValue();
        this.rpcAddress = new LazyInitializer<>(() -> new BJOutput<>(txout).getAddress().map(BJAddress::getName).map(RpcAddress::fromString));
    }

    @NonNull
    @Override
    public Optional<RpcAddress> getAddress() {
        return rpcAddress.get();
    }
}
