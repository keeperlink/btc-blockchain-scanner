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

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author Sliva Co
 */
public class MapWrapper {

    public final Map m;

    public MapWrapper(Map m) {
        this.m = m;
    }

    public boolean mapBool(String key) {
        return mapBool(m, key);
    }

    public float mapFloat(String key) {
        return mapFloat(m, key);
    }

    public double mapDouble(String key) {
        return mapDouble(m, key);
    }

    public int mapInt(String key) {
        return mapInt(m, key);
    }

    public long mapLong(String key) {
        return mapLong(m, key);
    }

    public String mapStr(String key) {
        return mapStr(m, key);
    }

    public Date mapCTime(String key) {
        return mapCTime(m, key);
    }

    public BigDecimal mapBigDecimal(String key) {
        return mapBigDecimal(m, key);
    }

    public static boolean mapBool(Map m, String key) {
        return ((Boolean) m.get(key));
    }

    public static BigDecimal mapBigDecimal(Map m, String key) {
        return new BigDecimal((String) m.get(key));
    }

    public static float mapFloat(Map m, String key) {
        return ((Number) m.get(key)).floatValue();
    }

    public static double mapDouble(Map m, String key) {
        return ((Number) m.get(key)).doubleValue();
    }

    public static int mapInt(Map m, String key) {
        return ((Number) m.get(key)).intValue();
    }

    public static long mapLong(Map m, String key) {
        return ((Number) m.get(key)).longValue();
    }

    public static String mapStr(Map m, String key) {
        Object v = m.get(key);
        return v == null ? null : String.valueOf(v);
    }

    public static Date mapCTime(Map m, String key) {
        Object v = m.get(key);
        return v == null ? null : new Date(mapLong(m, key) * 1000);
    }

    @Override
    public String toString() {
        return String.valueOf(m);
    }
}
