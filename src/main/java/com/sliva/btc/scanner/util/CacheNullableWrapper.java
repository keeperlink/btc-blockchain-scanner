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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Wrapper for guava cache to handle nulls as Optional.empty.
 *
 * @author Sliva Co
 * @param <K> Cache key type
 * @param <T> Cache value type
 */
@RequiredArgsConstructor
public class CacheNullableWrapper<K, T> implements Cache<K, Optional<T>> {

    private final Cache<K, Optional<T>> cache;

    @Override
    @NonNull
    public Optional<T> getIfPresent(Object key) {
        Optional<T> result = cache.getIfPresent(key);
        return result == null ? Optional.empty() : result;
    }

    public boolean isPresent(Object key) {
        return cache.getIfPresent(key) != null;
    }

    @Override
    @NonNull
    public Optional<T> get(K key, Callable<? extends Optional<T>> loader) throws ExecutionException {
        return cache.get(key, loader);
    }

    @Override
    @NonNull
    public ImmutableMap<K, Optional<T>> getAllPresent(Iterable<?> keys) {
        return cache.getAllPresent(keys);
    }

    @Override
    public void put(K key, Optional<T> value) {
        cache.put(key, value);
    }

    public void putNullableValue(K key, T value) {
        cache.put(key, Optional.ofNullable(value));
    }

    @Override
    public void putAll(Map<? extends K, ? extends Optional<T>> m) {
        cache.putAll(m);
    }

    @Override
    public void invalidate(Object key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public long size() {
        return cache.size();
    }

    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    @Override
    @NonNull
    public ConcurrentMap<K, Optional<T>> asMap() {
        return cache.asMap();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

}
