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
package com.sliva.btc.scanner.db.model;

import org.apache.commons.codec.binary.Hex;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Sliva Co
 */
public class Binary32Test {

    private static final String VALUE_1 = "0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca";
    private static final String VALUE_2 = "ed6ad1f896ff8221cfca585acf365c64b346dfc124fd9105de48e9fd22148d7b";

    @Test
    public void testInstance() throws Exception {
        Binary32 result = new Binary32(Hex.decodeHex(VALUE_1));
        assertEquals(VALUE_1, result.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInstance_fail_null() throws Exception {
        Binary32 result = new Binary32(null);
        assertEquals(VALUE_1, result.toString());
    }

    @Test
    public void testBuild() throws Exception {
        Binary32 expResult = new Binary32(Hex.decodeHex(VALUE_1));
        Binary32 result = Binary32.build(VALUE_1);
        assertEquals(expResult, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuild_fail_null() throws Exception {
        Binary32.build(null);
    }

    @Test
    public void testToString() {
        Binary32 instance = Binary32.build(VALUE_1);
        String expResult = VALUE_1;
        String result = instance.toString();
        assertEquals(expResult, result);
    }

    @Test
    public void testGetData() throws Exception {
        Binary32 instance = Binary32.build(VALUE_1);
        byte[] expResult = Hex.decodeHex(VALUE_1);
        byte[] result = instance.getData();
        assertArrayEquals(expResult, result);
    }

    @Test
    public void testEquals() {
        Object o = Binary32.build(VALUE_1);
        Binary32 instance = Binary32.build(VALUE_1);
        boolean expResult = true;
        boolean result = instance.equals(o);
        assertEquals(expResult, result);
    }

    @Test
    public void testEquals_not() {
        Object o = Binary32.build(VALUE_1);
        Binary32 instance = Binary32.build(VALUE_2);
        boolean expResult = false;
        boolean result = instance.equals(o);
        assertEquals(expResult, result);
    }

    @Test
    public void testCanEqual() {
        Object other = Binary32.build(VALUE_1);
        Binary32 instance = Binary32.build(VALUE_2);
        boolean expResult = true;
        boolean result = instance.canEqual(other);
        assertEquals(expResult, result);
    }

    @Test
    public void testHashCode() {
        Binary32 instance = Binary32.build(VALUE_2);
        int result = instance.hashCode();
        assertTrue(result != 0);
    }
}
