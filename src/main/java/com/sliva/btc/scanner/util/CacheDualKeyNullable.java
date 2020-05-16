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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import lombok.NonNull;

/**
 * Dual key with Optional values cache implementation.
 *
 * @param <K1> Key1 type
 * @param <K2> Key2 type
 * @param <T> Value type
 *
 * @author Sliva Co
 */
public class CacheDualKeyNullable<K1, K2, T> {

    private boolean active = true;
    private final Cache<K1, Optional<T>> cache1;
    private final Cache<K2, Optional<T>> cache2;
    private final Function<T, K1> key1extractor;
    private final Function<T, K2> key2extractor;

    public CacheDualKeyNullable(Function<CacheBuilder<Object, Object>, CacheBuilder<Object, Object>> cacheBuilder, Function<T, K1> key1extractor, Function<T, K2> key2extractor) {
        this.cache1 = cacheBuilder.apply(CacheBuilder.newBuilder())
                .removalListener(new RemovalListener1())
                .build();
        this.cache2 = cacheBuilder.apply(CacheBuilder.newBuilder())
                .removalListener(new RemovalListener2())
                .build();
        this.key1extractor = key1extractor;
        this.key2extractor = key2extractor;
    }

    public void put(T value) {
        checkArgument(value != null, "Argument 'value' is null");
        Optional<T> ovalue = Optional.of(value);
        putIfKeyNotNull1(key1extractor.apply(value), ovalue);
        putIfKeyNotNull2(key2extractor.apply(value), ovalue);
    }

    public void invalidate1(K1 key) {
        checkArgument(key != null, "Argument 'key' is null");
        cache1.invalidate(key);
    }

    public void invalidate2(K2 key) {
        checkArgument(key != null, "Argument 'key' is null");
        cache2.invalidate(key);
    }

    public void invalidateAll() {
        active = false;
        cache1.invalidateAll();
        cache2.invalidateAll();
        active = true;
    }

    @NonNull
    public Optional<T> getIfPresent1(K1 key) {
        checkArgument(key != null, "Argument 'key' is null");
        Optional<T> result = cache1.getIfPresent(key);
        return result == null ? Optional.empty() : result;
    }

    @NonNull
    public Optional<T> getIfPresent2(K2 key) {
        checkArgument(key != null, "Argument 'key' is null");
        Optional<T> result = cache2.getIfPresent(key);
        return result == null ? Optional.empty() : result;
    }

    public boolean isPresent1(K1 key) {
        checkArgument(key != null, "Argument 'key' is null");
        return cache1.getIfPresent(key) != null;
    }

    public boolean isPresent2(K2 key) {
        checkArgument(key != null, "Argument 'key' is null");
        return cache2.getIfPresent(key) != null;
    }

    @NonNull
    public Optional<T> get1(K1 key, Callable<Optional<T>> loader) throws ExecutionException {
        checkArgument(key != null, "Argument 'key' is null");
        checkArgument(loader != null, "Argument 'loader' is null");
        return cache1.get(key, () -> update2(loader.call()));
    }

    @NonNull
    public Optional<T> get2(K2 key, Callable<Optional<T>> loader) throws ExecutionException {
        checkArgument(key != null, "Argument 'key' is null");
        checkArgument(loader != null, "Argument 'loader' is null");
        return cache2.get(key, () -> update1(loader.call()));
    }

    @NonNull
    private Optional<T> update1(Optional<T> value) {
        checkArgument(value != null, "Argument 'loader' is null");
        value.ifPresent(t -> putIfKeyNotNull1(key1extractor.apply(t), value));
        return value;
    }

    @NonNull
    private Optional<T> update2(Optional<T> value) {
        checkArgument(value != null, "Argument 'loader' is null");
        value.ifPresent(t -> putIfKeyNotNull2(key2extractor.apply(t), value));
        return value;
    }

    private void putIfKeyNotNull1(K1 key, Optional<T> value) {
        if (key != null) {
            cache1.put(key, value);
        }
    }

    private void putIfKeyNotNull2(K2 key, Optional<T> value) {
        if (key != null) {
            cache2.put(key, value);
        }
    }

    private class RemovalListener1 implements RemovalListener<K1, Optional<T>> {

        private void remove(K2 key) {
            if (key != null) {
                cache2.invalidate(key);
            }
        }

        @Override
        public void onRemoval(RemovalNotification<K1, Optional<T>> notification) {
            if (active) {
                notification.getValue().map(key2extractor).ifPresent(this::remove);
            }
        }
    }

    private class RemovalListener2 implements RemovalListener<K2, Optional<T>> {

        private void remove(K1 key) {
            if (key != null) {
                cache1.invalidate(key);
            }
        }

        @Override
        public void onRemoval(RemovalNotification<K2, Optional<T>> notification) {
            if (active) {
                notification.getValue().map(key1extractor).ifPresent(this::remove);
            }
        }
    }
}
