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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.ToString;
import org.bitcoinj.core.Transaction;

/**
 *
 * @author Sliva Co
 */
@ToString
public class BJTransaction implements SrcTransaction<BJInput, BJOutput> {

    private final Transaction t;

    public BJTransaction(Transaction transaction) {
        this.t = transaction;
    }

    @Override
    public String getTxid() {
        return t.getHashAsString();
    }

    @Override
    public Stream<BJInput> getInputs() {
        if (t.isCoinBase()) {
            return null;
        }
        final AtomicInteger pos = new AtomicInteger(0);
        return t.getInputs().stream().map((i) -> new BJInput(i, pos.getAndIncrement()));
    }

    @Override
    public Stream<BJOutput> getOutputs() {
        return t.getOutputs().stream().map((o) -> new BJOutput(o));
    }

}
