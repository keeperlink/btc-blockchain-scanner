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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.ToString;
import org.bitcoinj.core.Transaction;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.In;

/**
 *
 * @author Sliva Co
 */
@ToString(doNotUseGetters = true)
public class RpcTransaction implements SrcTransaction<RpcInput, RpcOutput> {

    public static final String TRANSACTION_ZERO = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b";

    private final String txid;
    private Transaction tran;
    private RawTransaction rawTransaction;

    public RpcTransaction(String txid) {
        this.txid = txid;
    }

    public RpcTransaction(Transaction tran) {
        this.tran = tran;
        this.txid = tran.getHashAsString();
    }

    @Override
    public String getTxid() {
        return txid;
    }

    @Override
    public Stream<RpcInput> getInputs() {
        if (TRANSACTION_ZERO.equalsIgnoreCase(txid)) {
            //Bitcoin Core RPC does not return first transaction - generate it here
            return null;
        }
        if (tran != null) {
            if (tran.isCoinBase()) {
                return null;
            }
        } else {
            List<In> ins = getRawTransaction().vIn();
            if (ins == null || ins.isEmpty() || ins.get(0).txid() == null) {
                return null;
            }
        }
        final AtomicInteger pos = new AtomicInteger(0);
        if (tran != null) {
            return tran.getInputs().stream().map((t) -> new RpcInput(t, pos.getAndIncrement()));
        } else {
            return getRawTransaction().vIn().stream().map((t) -> new RpcInput(t, pos.getAndIncrement()));
        }
    }

    @Override
    public Stream<RpcOutput> getOutputs() {
        if (TRANSACTION_ZERO.equalsIgnoreCase(txid)) {
            //Bitcoin Core RPC does not return first transaction - generate it here
            return Collections.singletonList(new RpcOutput(new RawTransaction.Out() {
                @Override
                public double value() {
                    return 50;
                }

                @Override
                public int n() {
                    return 0;
                }

                @Override
                public ScriptPubKey scriptPubKey() {
                    return new ScriptPubKey() {
                        @Override
                        public String asm() {
                            throw new UnsupportedOperationException("Not supported");
                        }

                        @Override
                        public String hex() {
                            throw new UnsupportedOperationException("Not supported");
                        }

                        @Override
                        public int reqSigs() {
                            throw new UnsupportedOperationException("Not supported");
                        }

                        @Override
                        public String type() {
                            throw new UnsupportedOperationException("Not supported");
                        }

                        @Override
                        public List<String> addresses() {
                            return Collections.singletonList("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa");
                        }
                    };
                }

                @Override
                public BitcoindRpcClient.TxInput toInput() {
                    throw new UnsupportedOperationException("Not supported");
                }

                @Override
                public RawTransaction transaction() {
                    throw new UnsupportedOperationException("Not supported");
                }
            })).stream();
        }
        if (tran != null) {
            return tran.getOutputs().stream().map(t -> new RpcOutput(t));
        } else {
            return getRawTransaction().vOut().stream().map(t -> new RpcOutput(t));
        }
    }

    private RawTransaction getRawTransaction() {
        if (rawTransaction == null) {
            rawTransaction = RpcClient.getInstance().getRawTransaction(txid);
        }
        return rawTransaction;
    }
}
