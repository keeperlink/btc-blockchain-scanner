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

import com.sliva.btc.scanner.rpc.RpcClientDirect;
import com.sliva.btc.scanner.util.BJBlockHandler;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.ToString;
import org.bitcoinj.core.Block;
import org.spongycastle.util.encoders.Hex;

/**
 *
 * @author Sliva Co
 */
@ToString
public class RpcBlock<T extends RpcTransaction<RpcInput, RpcOutput<RpcAddress>>> implements SrcBlock<RpcTransaction<RpcInput, RpcOutput<RpcAddress>>> {

    private final Block block;
    private Integer blockHeight;

    public RpcBlock(int blockHeight) {
        try {
            this.block = getBlock(RpcClientDirect.getInstance().getBlockHash(blockHeight));
            this.blockHeight = blockHeight;
        } catch (IOException e) {
            throw new RuntimeException("blockHeight=" + blockHeight, e);
        }
    }

    public RpcBlock(String hash) {
        this.block = getBlock(hash);
    }

    private Block getBlock(String hash) {
        try {
            return BJBlockHandler.parseBlcok(Hex.decode(RpcClientDirect.getInstance().getRawBlock(hash)));
        } catch (IOException e) {
            throw new RuntimeException("blockHash=" + hash, e);
        }
    }

    @Override
    public String getHash() {
        return block.getHashAsString();
    }

    @Override
    public int getHeight() {
        if (blockHeight == null) {
            try {
                blockHeight = RpcClientDirect.getInstance().getBlockHeight(block.getHashAsString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return blockHeight;
    }

    @Override
    public Collection<RpcTransaction<RpcInput, RpcOutput<RpcAddress>>> getTransactions() {
        return Optional.ofNullable(block.getTransactions()).map(tx -> tx.stream().map(t -> new RpcTransaction<>(t)).collect(Collectors.toList())).orElse(Collections.EMPTY_LIST);
    }
}
