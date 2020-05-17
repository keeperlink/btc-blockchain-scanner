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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 *
 * @author Sliva Co
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BatchUtils {

    /**
     * Pull and remove data elements from source Collection up to provided
     * limit.
     *
     * @param <T> Element type in collection
     * @param source source Collection of elements
     * @param limit maximum number of elements to return
     * @return Optional of Collection of pulled elements or empty if source
     * Collection is empty.
     */
    public static <T> Optional<Collection<T>> pullData(Collection<T> source, int limit) {
        checkArgument(source != null, "Argument 'source' is null");
        checkArgument(limit > 0, "Argument 'limit' (%s) must be a positive number", limit);
        Optional<Collection<T>> result = Optional.empty();
        synchronized (source) {
            if (!source.isEmpty()) {
                if (source.size() <= limit) {
                    result = Optional.of(new ArrayList<>(source));
                    source.clear();
                } else {
                    Collection<T> pulled = source.stream().limit(limit).collect(Collectors.toList());
                    source.removeAll(pulled);
                    result = Optional.of(pulled);
                }
            }
            source.notifyAll();
        }
        return result;
    }
}
