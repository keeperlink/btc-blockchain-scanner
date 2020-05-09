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
package com.sliva.btc.scanner.db.model;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author whost
 */
public class SighashTypeTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSomeMethod() {
        System.out.println("SIGHASH_ALL=" + SighashType.toHexString(SighashType.SIGHASH_ALL));
        System.out.println("SIGHASH_ALL+ANYONECANPAY=" + SighashType.toHexString(SighashType.SIGHASH_ALL_ANYONECANPAY));
    }

    /**
     * Test of toHexString method, of class SighashType.
     */
    @Test
    public void testToHexString() {
        System.out.println("toHexString");
        byte b = SighashType.SIGHASH_ALL | SighashType.SIGHASH_ANYONECANPAY;
        String expResult = "81";
        String result = SighashType.toHexString(b);
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

    /**
     * Test of toHexString method, of class SighashType.
     */
    @Test
    public void testToHexString2() {
        System.out.println("toHexString2");
        byte b = SighashType.SIGHASH_NONE;
        String expResult = "02";
        String result = SighashType.toHexString(b);
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

}
