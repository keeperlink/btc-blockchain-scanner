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
package com.sliva.btc.scanner.db.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;

/**
 *
 * @author Sliva Co
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DbResultSetUtils {

    /**
     * Process ResultSet by calling consumer on each record.
     *
     * @param rs ResultSet object, will be closed at the end
     * @param consumer Record processor to be called
     * @return number of records processed
     */
    @SneakyThrows(SQLException.class)
    public static int executeQuery(ResultSet rs, QueryConsumer consumer) {
        AtomicInteger result = new AtomicInteger();
        try {
            while (rs.next()) {
                consumer.accept(rs);
                result.incrementAndGet();
            }
        } finally {
            rs.close();
        }
        return result.get();
    }

    /**
     * Process ResultSet by calling processor on each record and collection
     * results in list.
     *
     * @param <T> Value type returned by processor and passed through to return
     * list
     * @param rs ResultSet object, will be closed at the end
     * @param processor Record processor to be called
     * @return List of objects returned by the processor.
     */
    @NonNull
    @SneakyThrows(SQLException.class)
    public static <T> List<T> executeQueryToList(ResultSet rs, QueryResultProcessor<T> processor) {
        List<T> result = new ArrayList<>();
        try {
            while (rs.next()) {
                result.add(processor.apply(rs));
            }
        } finally {
            rs.close();
        }
        return result;
    }

    /**
     * Query a single row from ResultSet and call processor with the ResultSet.
     *
     * @param <T> Value type returned by processor and passed through to return
     * @param rs ResultSet object, will be closed at the end
     * @param processor Record processor to be called
     * @return Value returned by processor or empty if no record processed
     */
    @NonNull
    @SneakyThrows(SQLException.class)
    public static <T> Optional<T> querySingleRow(ResultSet rs, QueryResultProcessor<T> processor) {
        try {
            if (rs.next()) {
                return Optional.ofNullable(processor.apply(rs));
            }
        } finally {
            rs.close();
        }
        return Optional.empty();
    }

    public interface QueryConsumer {

        void accept(ResultSet rs) throws SQLException;
    }

    public interface QueryResultProcessor<T> {

        T apply(ResultSet rs) throws SQLException;
    }
}
