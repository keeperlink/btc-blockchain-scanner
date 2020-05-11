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

import lombok.AllArgsConstructor;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
@AllArgsConstructor
public class DbInput implements SrcInput {

    private final DbBlockProvider blockProvider;
    private final short pos;
    private final short inPos;
    private final int inTransactionId;
    private String inTxid;
    private final byte sighashType;
    private final boolean segwit;
    private final boolean multisig;

    @Override
    public short getPos() {
        return pos;
    }

    @Override
    public String getInTxid() {
        if (inTxid == null) {
            inTxid = blockProvider.psQueryTransactionHash.setParameters(p -> p.setInt(inTransactionId)).querySingleRow(rs -> Hex.encodeHexString(rs.getBytes(1), true)).orElse(null);
        }
        return inTxid;
    }

    @Override
    public short getInPos() {
        return inPos;
    }

    @Override
    public byte getSighashType() {
        return sighashType;
    }

    @Override
    public boolean isSegwit() {
        return segwit;
    }

    @Override
    public boolean isMultisig() {
        return multisig;
    }

    public int getInTransactionId() {
        return inTransactionId;
    }

}
