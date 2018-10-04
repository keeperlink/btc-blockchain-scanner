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

import com.sliva.btc.scanner.db.model.SighashType;
import com.sliva.btc.scanner.util.SigUtils;
import lombok.ToString;
import org.apache.commons.codec.binary.Hex;
import org.bitcoinj.core.TransactionInput;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction.In;

/**
 *
 * @author Sliva Co
 */
@ToString
public class RpcInput implements SrcInput {

    private final In in;
    private final TransactionInput txin;
    private final short pos;

    public RpcInput(In in, short pos) {
        this.in = in;
        this.txin = null;
        this.pos = pos;
    }

    public RpcInput(TransactionInput txin, short pos) {
        this.in = null;
        this.txin = txin;
        this.pos = pos;
    }

    @Override
    public short getPos() {
        return pos;
    }

    @Override
    @SuppressWarnings("null")
    public String getInTxid() {
        return txin != null ? Hex.encodeHexString(txin.getOutpoint().getHash().getBytes(), true) : in.txid();
    }

    @Override
    @SuppressWarnings("null")
    public short getInPos() {
        return txin != null ? (short) txin.getOutpoint().getIndex() : (short) in.vout();
    }

    @Override
    public byte getSighashType() {
        return txin != null ? SigUtils.getSighashType(txin) : SighashType.UNDEFINED;
    }

    @Override
    public boolean isSegwit() {
        return txin != null ? txin.hasWitness() : in.scriptSig().containsKey("witness");
    }

    @Override
    public boolean isMultisig() {
        return txin != null ? SigUtils.isMultisig(txin) : in.scriptSig().size() > 1;
    }

    public In getIn() {
        return in;
    }
}
