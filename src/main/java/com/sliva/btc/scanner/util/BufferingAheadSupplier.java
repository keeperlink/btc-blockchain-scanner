/*
 * Copyright 2020 Sliva Co.
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
package com.sliva.btc.scanner.util;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Supplier that reads ahead and buffers results from provided supplier.Buffer
 * size is limited by constructor argument maxBufferSize. For finite data set,
 * in order to notify that all data elements been served, provided supplier
 * should throw NoSuchElementException on get() method. Accordingly, this
 * Supplier implementation will throw NoSuchElementException on get() when last
 * element from buffer been served and no more elements available from the
 * source supplier.
 *
 * #Thread Safe
 *
 * @author Sliva Co
 * @param <T> Data element type
 */
@RequiredArgsConstructor
@Slf4j
public class BufferingAheadSupplier<T> implements Supplier<T> {

    private final Supplier<T> supplier;
    private final int maxBufferSize;
    private final Queue<T> buffer = new LinkedList<>();

    @Override
    public T get() {
        long start = System.nanoTime();
        log.trace("BufferingAheadSupplier.get: STARTED. buffer.size={}", buffer.size());
        try {
            synchronized (buffer) {
                if (buffer.isEmpty()) {
                    supply();
                }
                T value = buffer.remove();
                log.trace("BufferingAheadSupplier.get: Value retrieved={}", value);
                supply();
                return value;
            }
        } finally {
            if (log.isTraceEnabled()) {
                log.trace("BufferingAheadSupplier.get: FINISHED. buffer.size={}, Runtime={} ms.", buffer.size(), new BigDecimal(System.nanoTime() - start).movePointLeft(6));
            }
        }
    }

    public void supply() {
        long start = System.nanoTime();
        log.trace("BufferingAheadSupplier.supply: STARTED. buffer.size={}", buffer.size());
        try {
            if (buffer.size() < maxBufferSize) {
                synchronized (buffer) {
                    while (buffer.size() < maxBufferSize) {
                        buffer.add(supplier.get());
                        log.trace("BufferingAheadSupplier.supply: buffer.size={}", buffer.size());
                    }
                }
            }
        } catch (NoSuchElementException ex) {
            log.trace("BufferingAheadSupplier.supply: No more elements.");
        } finally {
            if (log.isTraceEnabled()) {
                log.trace("BufferingAheadSupplier.supply: FINISHED. buffer.size={}, Runtime={} ms.", buffer.size(), new BigDecimal(System.nanoTime() - start).movePointLeft(6));
            }
        }
    }
}
