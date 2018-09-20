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
package com.sliva.btc.scanner.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Sliva Co
 */
public class IntCollection {

    private final int blockSize;
    private final List<IntArray> list = new ArrayList<>();

    public IntCollection(int blockSize) {
        this.blockSize = blockSize;
    }

    public void add(int value) {
        IntArray last = getLast();
        if (last == null || last.isFull()) {
            last = new IntArray();
            list.add(last);
        }
        last.add(value);
    }

    public int get(int pos) {
        return list.get(pos / blockSize).array[pos % blockSize];
    }

    public int getSize() {
        return list.isEmpty() ? 0 : (list.size() - 1) * blockSize + getLast().getSize();
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    private IntArray getLast() {
        return list.isEmpty() ? null : list.get(list.size() - 1);
    }

    private class IntArray {

        private final int[] array = new int[blockSize];
        private final AtomicInteger size = new AtomicInteger(0);

        private void add(int a) {
            if (isFull()) {
                throw new IllegalStateException("Array is full");
            }
            array[size.getAndIncrement()] = a;
        }

        private boolean isFull() {
            return size.get() == blockSize;
        }

        private int getSize() {
            return size.get();
        }

    }
}
