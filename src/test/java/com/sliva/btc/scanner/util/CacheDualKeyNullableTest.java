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

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Sliva Co
 */
public class CacheDualKeyNullableTest {

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    private static class DualKeyClass {

        private final String key1;
        private final Integer key2;
    }
    private static final int MAX_CACHE_SIZE = 1;
    CacheDualKeyNullable<String, Integer, DualKeyClass> instance = new CacheDualKeyNullable<>(
            true, true,
            b -> b.maximumSize(MAX_CACHE_SIZE),
            DualKeyClass::getKey1, DualKeyClass::getKey2);
    private final String key1 = "key1";
    private final Integer key2 = 1;
    private final DualKeyClass value = new DualKeyClass(key1, key2);

    public CacheDualKeyNullableTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testPut() {
        instance.put(value);
        assertTrue(instance.isPresent1(key1));
        assertTrue(instance.isPresent2(key2));
    }

    @Test
    public void testPut_key1_null() {
        instance.put(new DualKeyClass(null, key2));
        assertFalse(instance.isPresent1(key1));
        assertTrue(instance.isPresent2(key2));
    }

    @Test
    public void testPut_key2_null() {
        instance.put(new DualKeyClass(key1, null));
        assertTrue(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testPut__both_keys_null() {
        instance.put(new DualKeyClass(null, null));
        assertFalse(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testInvalidate1() {
        instance.invalidate1(key1);
    }

    @Test
    public void testInvalidate1_hadValue() {
        instance.put(value);
        instance.invalidate1(key1);
        assertFalse(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testInvalidate1_hadValue_otherKey_null() {
        instance.put(new DualKeyClass(key1, null));
        instance.invalidate1(key1);
        assertFalse(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testInvalidate2() {
        instance.invalidate2(key2);
    }

    @Test
    public void testInvalidate2_hadValue() {
        instance.put(value);
        instance.invalidate2(key2);
        assertFalse(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testInvalidate2_hadValue_otherKey_null() {
        instance.put(new DualKeyClass(null, key2));
        instance.invalidate2(key2);
        assertFalse(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testInvalidateAll() {
        instance.invalidateAll();
    }

    @Test
    public void testGetIfPresent1() {
        Optional<DualKeyClass> expResult = Optional.empty();
        Optional<DualKeyClass> result = instance.getIfPresent1(key1);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetIfPresent1_present() {
        instance.put(value);
        Optional<DualKeyClass> expResult = Optional.of(value);
        Optional<DualKeyClass> result = instance.getIfPresent1(key1);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetIfPresent2() {
        Optional<DualKeyClass> expResult = Optional.empty();
        Optional<DualKeyClass> result = instance.getIfPresent2(key2);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetIfPresent2_present() {
        instance.put(value);
        Optional<DualKeyClass> expResult = Optional.of(value);
        Optional<DualKeyClass> result = instance.getIfPresent2(key2);
        assertEquals(expResult, result);
    }

    @Test
    public void testIsPresent1() {
        boolean expResult = false;
        boolean result = instance.isPresent1(key1);
        assertEquals(expResult, result);
    }

    @Test
    public void testIsPresent2() {
        boolean expResult = false;
        boolean result = instance.isPresent2(key2);
        assertEquals(expResult, result);
    }

    @Test
    public void testGet1() throws Exception {
        Optional<DualKeyClass> expResult = Optional.of(value);
        Optional<DualKeyClass> result = instance.get1(key1, k -> Optional.of(value));
        assertEquals(expResult, result);
        assertTrue(instance.isPresent1(key1));
        assertTrue(instance.isPresent2(key2));
    }

    @Test
    public void testGet1_otherKey_null() throws Exception {
        Optional<DualKeyClass> expResult = Optional.of(new DualKeyClass(key1, null));
        Optional<DualKeyClass> result = instance.get1(key1, k -> Optional.of(new DualKeyClass(key1, null)));
        assertEquals(expResult, result);
        assertTrue(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testGet1_empty() throws Exception {
        Optional<DualKeyClass> expResult = Optional.empty();
        Optional<DualKeyClass> result = instance.get1(key1, k -> Optional.empty());
        assertEquals(expResult, result);
        assertTrue(instance.isPresent1(key1));
        assertFalse(instance.isPresent2(key2));
    }

    @Test
    public void testGet2() throws Exception {
        Optional<DualKeyClass> expResult = Optional.of(value);
        Optional<DualKeyClass> result = instance.get2(key2, k -> Optional.of(value));
        assertEquals(expResult, result);
        assertTrue(instance.isPresent1(key1));
        assertTrue(instance.isPresent2(key2));
    }

    @Test
    public void testGet2_otherKey_null() throws Exception {
        Optional<DualKeyClass> expResult = Optional.of(new DualKeyClass(null, key2));
        Optional<DualKeyClass> result = instance.get2(key2, k -> Optional.of(new DualKeyClass(null, key2)));
        assertEquals(expResult, result);
        assertFalse(instance.isPresent1(key1));
        assertTrue(instance.isPresent2(key2));
    }

    @Test
    public void testGet2_empty() throws Exception {
        Optional<DualKeyClass> expResult = Optional.empty();
        Optional<DualKeyClass> result = instance.get2(key2, k -> Optional.empty());
        assertEquals(expResult, result);
        assertFalse(instance.isPresent1(key1));
        assertTrue(instance.isPresent2(key2));
    }
}
