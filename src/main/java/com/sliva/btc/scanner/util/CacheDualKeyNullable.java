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
import static com.google.common.base.Preconditions.checkState;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.Optional;
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

    public CacheDualKeyNullable(boolean cache1, boolean cache2, Function<CacheBuilder<Object, Object>, CacheBuilder<Object, Object>> cacheBuilder, Function<T, K1> key1extractor, Function<T, K2> key2extractor) {
        checkArgument(cacheBuilder != null, "Argument 'cacheBuilder' is null");
        checkArgument(key1extractor != null, "Argument 'key1extractor' is null");
        checkArgument(key2extractor != null, "Argument 'key2extractor' is null");
        this.cache1 = cache1 ? cacheBuilder.apply(CacheBuilder.newBuilder())
                .removalListener(new RemovalListener1())
                .build() : null;
        this.cache2 = cache2 ? cacheBuilder.apply(CacheBuilder.newBuilder())
                .removalListener(new RemovalListener2())
                .build() : null;
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
        checkState(cache1 != null, "Cache-1 is not enabled");
        cache1.invalidate(key);
    }

    public void invalidate2(K2 key) {
        checkArgument(key != null, "Argument 'key' is null");
        checkState(cache2 != null, "Cache-1 is not enabled");
        cache2.invalidate(key);
    }

    public void invalidate(T value) {
        checkArgument(value != null, "Argument 'value' is null");
        K1 key1 = key1extractor.apply(value);
        if (key1 != null && cache1 != null) {
            invalidate1(key1);
        }
        K2 key2 = key2extractor.apply(value);
        if (key2 != null && cache2 != null) {
            invalidate2(key2);
        }
    }

    public void invalidateAll() {
        active = false;
        if (cache1 != null) {
            cache1.invalidateAll();
        }
        if (cache2 != null) {
            cache2.invalidateAll();
        }
        active = true;
    }

    @NonNull
    public Optional<T> getIfPresent1(K1 key) {
        checkArgument(key != null, "Argument 'key' is null");
        checkState(cache1 != null, "Cache-1 is not enabled");
        Optional<T> result = cache1.getIfPresent(key);
        return result == null ? Optional.empty() : result;
    }

    @NonNull
    public Optional<T> getIfPresent2(K2 key) {
        checkArgument(key != null, "Argument 'key' is null");
        checkState(cache2 != null, "Cache-1 is not enabled");
        Optional<T> result = cache2.getIfPresent(key);
        return result == null ? Optional.empty() : result;
    }

    public boolean isPresent1(K1 key) {
        checkArgument(key != null, "Argument 'key' is null");
        checkState(cache1 != null, "Cache-1 is not enabled");
        return cache1.getIfPresent(key) != null;
    }

    public boolean isPresent2(K2 key) {
        checkArgument(key != null, "Argument 'key' is null");
        checkState(cache2 != null, "Cache-1 is not enabled");
        return cache2.getIfPresent(key) != null;
    }

    @NonNull
    public Optional<T> get1(K1 key, Function<K1, Optional<T>> loader) throws ExecutionException {
        checkArgument(key != null, "Argument 'key' is null");
        checkArgument(loader != null, "Argument 'loader' is null");
        checkState(cache1 != null, "Cache-1 is not enabled");
        return cache1.get(key, () -> update2(loader.apply(key)));
    }

    @NonNull
    public Optional<T> get2(K2 key, Function<K2, Optional<T>> loader) throws ExecutionException {
        checkArgument(key != null, "Argument 'key' is null");
        checkArgument(loader != null, "Argument 'loader' is null");
        checkState(cache2 != null, "Cache-1 is not enabled");
        return cache2.get(key, () -> update1(loader.apply(key)));
    }

    public CacheStats getStats1() {
        checkState(cache1 != null, "Cache-1 is not enabled");
        return cache1.stats();
    }

    public CacheStats getStats2() {
        checkState(cache2 != null, "Cache-1 is not enabled");
        return cache2.stats();
    }

    @NonNull
    private Optional<T> update1(Optional<T> value) {
        checkArgument(value != null, "Argument 'loader' is null");
        if (cache1 != null) {
            value.ifPresent(t -> putIfKeyNotNull1(key1extractor.apply(t), value));
        }
        return value;
    }

    @NonNull
    private Optional<T> update2(Optional<T> value) {
        checkArgument(value != null, "Argument 'loader' is null");
        if (cache2 != null) {
            value.ifPresent(t -> putIfKeyNotNull2(key2extractor.apply(t), value));
        }
        return value;
    }

    private void putIfKeyNotNull1(K1 key, Optional<T> value) {
        if (key != null && cache1 != null) {
            cache1.put(key, value);
        }
    }

    private void putIfKeyNotNull2(K2 key, Optional<T> value) {
        if (key != null && cache2 != null) {
            cache2.put(key, value);
        }
    }

    private class RemovalListener1 implements RemovalListener<K1, Optional<T>> {

        private void remove(K2 key) {
            if (key != null && cache2 != null) {
                cache2.invalidate(key);
            }
        }

        @Override
        public void onRemoval(RemovalNotification<K1, Optional<T>> notification) {
            if (active && cache2 != null) {
                notification.getValue().map(key2extractor).ifPresent(this::remove);
            }
        }
    }

    private class RemovalListener2 implements RemovalListener<K2, Optional<T>> {

        private void remove(K1 key) {
            if (key != null && cache1 != null) {
                cache1.invalidate(key);
            }
        }

        @Override
        public void onRemoval(RemovalNotification<K2, Optional<T>> notification) {
            if (active && cache1 != null) {
                notification.getValue().map(key1extractor).ifPresent(this::remove);
            }
        }
    }
}
