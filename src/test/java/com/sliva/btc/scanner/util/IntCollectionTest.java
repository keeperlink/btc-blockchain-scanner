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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author whost
 */
public class IntCollectionTest {

    public IntCollectionTest() {
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

    /**
     * Test of add method, of class IntCollection.
     */
    @Test
    public void testAdd() {
        System.out.println("add");
        IntCollection instance = new IntCollection(10);
        for (int i = 0; i < 115; i++) {
            instance.add(i * 2);
        }
        assertEquals(115, instance.getSize());
        for (int i = 0; i < 115; i++) {
            assertEquals(i * 2, instance.get(i));
        }
    }

    /**
     * Test of get method, of class IntCollection.
     */
    @Test
    public void testGet() {
        System.out.println("get");
        int pos = 0;
        IntCollection instance = new IntCollection(1);
        instance.add(5);
        int expResult = 5;
        int result = instance.get(pos);
        assertEquals(expResult, result);
    }

    /**
     * Test of getSize method, of class IntCollection.
     */
    @Test
    public void testGetSize() {
        System.out.println("getSize");
        IntCollection instance = new IntCollection(2);
        int expResult = 0;
        int result = instance.getSize();
        assertEquals(expResult, result);
    }

}
