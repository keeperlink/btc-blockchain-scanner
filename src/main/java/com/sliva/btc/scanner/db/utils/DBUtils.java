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
package com.sliva.btc.scanner.db.utils;

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.DbUpdate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author whost
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DBUtils {

    /**
     * Read integer value from single column result.
     *
     * @param ps PreparedStatement
     * @return Optional integer value or empty
     */
    @NonNull
    public static Optional<Integer> readInteger(DBPreparedStatement ps) {
        return ps.querySingleRow(rs -> rs.getObject(1, Integer.class));
    }

    /**
     * Read integer values from single column result set.
     *
     * @param ps PreparedStatement
     * @return Collection
     */
    @NonNull
    public static Set<Integer> readIntegersToSet(DBPreparedStatement ps) {
        Set<Integer> result = new HashSet<>();
        ps.executeQuery(rs -> result.add(rs.getObject(1, Integer.class)));
        return result;
    }

    /**
     * Read integer values from dual column result set.
     *
     * @param ps PreparedStatement
     * @return Map
     */
    @NonNull
    public static Map<Integer, Integer> readIntegersToMap(DBPreparedStatement ps) {
        Map<Integer, Integer> result = new HashMap<>();
        ps.executeQuery(rs -> result.put(rs.getObject(1, Integer.class), rs.getObject(2, Integer.class)));
        return result;
    }

    public static void truncateTables(DBConnectionSupplier con, DbUpdate... updates) {
        Stream.of(updates).map(DbUpdate::getTableName).forEach(name -> truncateTable(con, name));
    }

    public static void truncateTable(DBConnectionSupplier con, String tableName) {
        log.debug("Truncating table \"{}\"", tableName);
        con.prepareStatement("TRUNCATE TABLE `" + tableName + "`").execute();
    }
}
