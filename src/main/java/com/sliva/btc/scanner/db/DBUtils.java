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
package com.sliva.btc.scanner.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author whost
 */
public final class DBUtils {

    /**
     * Read integer values from single column result set.
     *
     * @param ps
     * @return
     * @throws SQLException
     */
    public static Collection<Integer> readIntegers(PreparedStatement ps) throws SQLException {
        Set<Integer> result = new HashSet<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Integer addressId = rs.getObject(1, Integer.class);
                result.add(addressId);
            }
        }
        return result;
    }

}
