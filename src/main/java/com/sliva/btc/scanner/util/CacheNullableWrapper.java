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
 * Wrapper for guava Cache interface to handle nulls as Optional.empty.
 *
 * @author Sliva Co
 * @param <K> Cache key type
 * @param <T> Cache value type
 */
@RequiredArgsConstructor
public class CacheNullableWrapper<K, T> implements Cache<K, Optional<T>> {

    private final Cache<K, Optional<T>> cache;

    /**
     * Returns the value associated with key in this cache, or empty if there is
     * no cached value for key. Note, that cached value can also be empty.
     *
     * @param key Key
     * @return Optional of cached value or empty
     */
    @Override
    @NonNull
    public Optional<T> getIfPresent(Object key) {
        Optional<T> result = cache.getIfPresent(key);
        return result == null ? Optional.empty() : result;
    }

    /**
     * Check if key is present in cache no matter if associated value is real or
     * empty.
     *
     * @param key Key
     * @return true if key is present in cache
     */
    public boolean isPresent(Object key) {
        return cache.getIfPresent(key) != null;
    }

    /**
     * Returns the value associated with key in this cache, obtaining that value
     * from loader if necessary. The method improves upon the conventional "if
     * cached, return; otherwise create, cache and return" pattern. Note, the
     * loader will not be called if value is present in cache, but empty.
     *
     * @param key Key
     * @param loader Loader to be called if key is not present in cache
     * @return Optional value associated with key
     * @throws ExecutionException if a checked exception was thrown while
     * loading the value
     */
    @Override
    @NonNull
    public Optional<T> get(K key, Callable<? extends Optional<T>> loader) throws ExecutionException {
        return cache.get(key, loader);
    }

    /**
     * Returns a map of the values associated with keys in this cache. The
     * returned map will only contain entries which are already present in the
     * cache.
     *
     * @param keys Key
     * @return a map of the values associated with keys in this cache
     */
    @Override
    @NonNull
    public ImmutableMap<K, Optional<T>> getAllPresent(Iterable<?> keys) {
        return cache.getAllPresent(keys);
    }

    /**
     * Associates value with key in this cache. If the cache previously
     * contained a value associated with key, the old value is replaced by
     * value.
     *
     * @param key Key
     * @param value Value
     */
    @Override
    public void put(K key, Optional<T> value) {
        cache.put(key, value);
    }

    /**
     * Associates value with key in this cache. If the cache previously
     * contained a value associated with key, the old value is replaced by
     * value. if value is null, then put in cache Optional.empty
     *
     * @param key Key
     * @param value Value
     */
    public void putNullableValue(K key, T value) {
        cache.put(key, Optional.ofNullable(value));
    }

    /**
     * Copies all of the mappings from the specified map to the cache. The
     * effect of this call is equivalent to that of calling put(k, v) on this
     * map once for each mapping from key k to value v in the specified map. The
     * behavior of this operation is undefined if the specified map is modified
     * while the operation is in progress.
     *
     * @param m Map
     */
    @Override
    public void putAll(Map<? extends K, ? extends Optional<T>> m) {
        cache.putAll(m);
    }

    /**
     * Discards any cached value for key key.
     *
     * @param key Key
     */
    @Override
    public void invalidate(Object key) {
        cache.invalidate(key);
    }

    /**
     * Discards any cached values for keys keys.
     *
     * @param keys
     */
    @Override
    public void invalidateAll(Iterable<?> keys) {
        cache.invalidateAll(keys);
    }

    /**
     * Discards all entries in the cache.
     */
    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    /**
     * Returns the approximate number of entries in this cache.
     *
     * @return the approximate number of entries in this cache
     */
    @Override
    public long size() {
        return cache.size();
    }

    /**
     * Returns a current snapshot of this cache's cumulative statistics, or a
     * set of default values if the cache is not recording statistics. All
     * statistics begin at zero and never decrease over the lifetime of the
     * cache.
     *
     * @return CacheStats instance
     */
    @Override
    public CacheStats stats() {
        return cache.stats();
    }

    /**
     * Returns a view of the entries stored in this cache as a thread-safe map.
     * Modifications made to the map directly affect the cache.
     *
     * @return Map
     */
    @Override
    @NonNull
    public ConcurrentMap<K, Optional<T>> asMap() {
        return cache.asMap();
    }

    /**
     * Performs any pending maintenance operations needed by the cache. Exactly
     * which activities are performed -- if any -- is implementation-dependent.
     */
    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

}
