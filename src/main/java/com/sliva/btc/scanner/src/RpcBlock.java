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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import lombok.ToString;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;

/**
 *
 * @author Sliva Co
 */
@ToString
public class RpcBlock implements SrcBlock {

    private final BitcoindRpcClient.Block block;
    private final Map<String, SrcTransaction> cache = new HashMap<>();

    public RpcBlock(int blockHeight) {
        this.block = RpcClient.getInstance().getBlock(blockHeight);
    }

    public RpcBlock(String hash) {
        this.block = RpcClient.getInstance().getBlock(hash);
    }

    @Override
    public String getHash() {
        return block.hash();
    }

    @Override
    public int getHeight() {
        return block.height();
    }

    @Override
    public Stream<SrcTransaction> getTransactions() {
        return block.tx().stream().map(t -> getTx(t));
    }

    private SrcTransaction getTx(String txid) {
        synchronized (cache) {
            SrcTransaction tx = cache.get(txid);
            if (tx == null) {
                tx = new RpcTransaction(txid);
                cache.put(txid, tx);
            }
            return tx;
        }
    }
}
