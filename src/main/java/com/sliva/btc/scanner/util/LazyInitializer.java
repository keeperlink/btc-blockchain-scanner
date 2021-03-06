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

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.lang3.concurrent.ConcurrentException;

/**
 *
 * @author Sliva Co
 * @param <T>
 */
@RequiredArgsConstructor
public class LazyInitializer<T> extends org.apache.commons.lang3.concurrent.LazyInitializer<T> {

    private final Supplier<T> supplier;

    @Override
    protected T initialize() {
        return supplier.get();
    }

    @Override
    @SneakyThrows(ConcurrentException.class)
    public T get() {
        return super.get();
    }
}
