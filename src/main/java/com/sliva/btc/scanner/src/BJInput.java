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
import org.bitcoinj.core.TransactionInput;

/**
 *
 * @author Sliva Co
 */
@ToString
public class BJInput implements SrcInput {

    private final TransactionInput ti;
    private final int pos;

    public BJInput(TransactionInput ti, int pos) {
        this.ti = ti;
        this.pos = pos;
    }

    @Override
    public int getPos() {
        return pos;
    }

    @Override
    public String getInTxid() {
        return ti.getOutpoint().getHash().toString();
    }

    @Override
    public int getInPos() {
        return (int) ti.getOutpoint().getIndex();
    }

}
