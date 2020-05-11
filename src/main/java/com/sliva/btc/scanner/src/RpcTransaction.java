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
import static com.google.common.base.Preconditions.checkNotNull;
import com.sliva.btc.scanner.rpc.RpcClient;
import com.sliva.btc.scanner.util.LazyInitializer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.bitcoinj.core.Transaction;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.In;

/**
 *
 * @author Sliva Co
 * @param <I>
 * @param <O>
 */
@ToString(doNotUseGetters = true)
public class RpcTransaction<I extends RpcInput, O extends RpcOutput<RpcAddress>> implements SrcTransaction<RpcInput, RpcOutput<RpcAddress>> {

    public static final String TRANSACTION_ZERO_ID = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b";
    private static final Collection<RpcInput> EMPTY_LIST = new ArrayList<>(0);

    private final String txid;
    private Transaction tran;
    private final LazyInitializer<RawTransaction> rawTransaction;

    public RpcTransaction(String txid) {
        this(null, checkNotNull(txid));
    }

    public RpcTransaction(Transaction tran) {
        this(checkNotNull(tran), tran.getHashAsString());
    }

    private RpcTransaction(Transaction tran, String txid) {
        checkArgument(txid != null, "Argument 'txid' is null");
        this.tran = tran;
        this.txid = txid;
        rawTransaction = new LazyInitializer<>(() -> RpcClient.getInstance().getRawTransaction(txid));
    }

    @Override
    public String getTxid() {
        return txid;
    }

    @NonNull
    @Override
    public Collection<RpcInput> getInputs() {
        if (TRANSACTION_ZERO_ID.equalsIgnoreCase(txid)) {
            //Bitcoin Core RPC does not return first transaction - generate it here
            return EMPTY_LIST;
        }
        if (tran != null) {
            if (tran.isCoinBase()) {
                return EMPTY_LIST;
            }
        } else {
            List<In> ins = getRawTransaction().vIn();
            if (ins == null || ins.isEmpty() || ins.get(0).txid() == null) {
                return EMPTY_LIST;
            }
        }
        final AtomicInteger pos = new AtomicInteger(0);
        if (tran != null) {
            return tran.getInputs().stream().map((t) -> new RpcInput(t, (short) pos.getAndIncrement())).collect(Collectors.toList());
        } else {
            return getRawTransaction().vIn().stream().map(t -> new RpcInput(t, (short) pos.getAndIncrement())).collect(Collectors.toList());
        }
    }

    @NonNull
    @Override
    public Collection<RpcOutput<RpcAddress>> getOutputs() {
        if (TRANSACTION_ZERO_ID.equalsIgnoreCase(txid)) {
            //Bitcoin Core RPC does not return first transaction - generate it here
            return Collections.singletonList(buildTransactionZero());
        }
        if (tran != null) {
            return tran.getOutputs().stream().map(t -> new RpcOutput<>(t)).collect(Collectors.toList());
        } else {
            return getRawTransaction().vOut().stream().map(t -> new RpcOutput<>(t)).collect(Collectors.toList());
        }
    }

    @NonNull
    @SneakyThrows(ConcurrentException.class)
    private RawTransaction getRawTransaction() {
        return rawTransaction.get();
    }

    @NonNull
    private RpcOutput<RpcAddress> buildTransactionZero() {
        return new RpcOutput<>(new RawTransaction.Out() {
            @Override
            public BigDecimal value() {
                return new BigDecimal("50");
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
        });
    }
}
