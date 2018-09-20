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
package com.sliva.btc.scanner.rpc;

import com.sliva.btc.scanner.util.MapWrapper;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.ToString;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Block;
import wf.bitcoin.javabitcoindrpcclient.GenericRpcException;

/**
 *
 * @author Sliva Co
 */
@ToString
public class BlockMapWrapper extends MapWrapper implements Block {

    public BlockMapWrapper(Map m) {
        super(m);
    }

    @Override
    public String hash() {
        return mapStr("hash");
    }

    @Override
    public int confirmations() {
        return mapInt("confirmations");
    }

    @Override
    public int size() {
        return mapInt("size");
    }

    @Override
    public int height() {
        return mapInt("height");
    }

    @Override
    public int version() {
        return mapInt("version");
    }

    @Override
    public String merkleRoot() {
        return mapStr("merkleroot");
    }

    @Override
    public String chainwork() {
        return mapStr("chainwork");
    }

    @Override
    public List<String> tx() {
        return (List<String>) m.get("tx");
    }

    @Override
    public Date time() {
        return mapCTime("time");
    }

    @Override
    public long nonce() {
        return mapLong("nonce");
    }

    @Override
    public String bits() {
        return mapStr("bits");
    }

    @Override
    public double difficulty() {
        return mapDouble("difficulty");
    }

    @Override
    public String previousHash() {
        return mapStr("previousblockhash");
    }

    @Override
    public String nextHash() {
        return mapStr("nextblockhash");
    }

    @Override
    public Block previous() throws GenericRpcException {
        return null;
    }

    @Override
    public Block next() throws GenericRpcException {
        return null;
    }
}
