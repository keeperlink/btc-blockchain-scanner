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

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Sliva Co
 */
public class TxOutputTest {

    private static final int TXN_ID = 111;
    private static final short TXN_POS = 2;
    private static final int ADDRESS_ID = 123;
    private static final long TXN_AMOUNT = 1000;
    private static final byte TXN_STATUS = 1;
    private final TxOutput instance = TxOutput.builder()
            .transactionId(TXN_ID)
            .pos(TXN_POS)
            .addressId(ADDRESS_ID)
            .amount(TXN_AMOUNT)
            .status(TXN_STATUS)
            .build();

    public TxOutputTest() {
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
     * Test of getAddressId method, of class TxOutput.
     */
    @Test
    public void testGetAddressId() {
        assertEquals(ADDRESS_ID, instance.getAddressId());
    }

    /**
     * Test of getAmount method, of class TxOutput.
     */
    @Test
    public void testGetAmount() {
        assertEquals(TXN_AMOUNT, instance.getAmount());
    }

    /**
     * Test of getStatus method, of class TxOutput.
     */
    @Test
    public void testGetStatus() {
        assertEquals(TXN_STATUS, instance.getStatus());
    }

    /**
     * Test of builder method, of class TxOutput.
     */
    @Test
    public void testBuilder() {
        TxOutput.TxOutputBuilder result = TxOutput.builder();
        assertNotNull(result);
        result.build();
    }

    /**
     * Test of toBuilder method, of class TxOutput.
     */
    @Test
    public void testToBuilder() {
        TxOutput.TxOutputBuilder result = instance.toBuilder();
        assertEquals(instance, result.build());
    }

    /**
     * Test of toString method, of class TxOutput.
     */
    @Test
    public void testToString() {
        System.out.println("toString");
        String result = instance.toString();
        System.out.println("testToString: result=" + result);
        assertTrue(result.contains("transactionId"));
    }

    @Test
    public void testEquals() {
        TxOutput instance2 = instance.toBuilder().pos(instance.getPos()).build();
        assertEquals(true, instance.equals(instance2));
    }

    @Test
    public void testEquals_yes_diff_addr() {
        TxOutput instance2 = instance.toBuilder().addressId(999).build();
        assertEquals(true, instance.equals(instance2));
    }

    @Test
    public void testEquals_yes_diff_amount() {
        TxOutput instance2 = instance.toBuilder().amount(999).build();
        assertEquals(true, instance.equals(instance2));
    }

    @Test
    public void testEquals_yes_diff_status() {
        TxOutput instance2 = instance.toBuilder().status((byte) 5).build();
        assertEquals(true, instance.equals(instance2));
    }

    @Test
    public void testEquals_no_diff_pos() {
        TxOutput instance2 = instance.toBuilder().pos((short) (instance.getPos() + 1)).build();
        assertEquals(false, instance.equals(instance2));
    }

    @Test
    public void testEquals_no_diff_transactionId() {
        TxOutput instance2 = instance.toBuilder().transactionId(instance.getTransactionId() + 1).build();
        assertEquals(false, instance.equals(instance2));
    }

    @Test
    public void testCompare() {
        TxOutput instance2 = instance.toBuilder().pos(instance.getPos()).build();
        assertEquals(0, instance2.compareTo(instance));
    }

    @Test
    public void testCompare_largerPos() {
        TxOutput instance2 = instance.toBuilder().pos((short) (instance.getPos() + 1)).build();
        assertEquals(1, instance2.compareTo(instance));
    }

    @Test
    public void testCompare_smallerPos() {
        TxOutput instance2 = instance.toBuilder().pos((short) (instance.getPos() - 1)).build();
        assertEquals(-1, instance2.compareTo(instance));
    }

    @Test
    public void testCompare_largerTransactionId() {
        TxOutput instance2 = instance.toBuilder().transactionId(instance.getTransactionId() + 1).build();
        assertEquals(1, instance2.compareTo(instance));
    }

    @Test
    public void testCompare_smallerTransactionId() {
        TxOutput instance2 = instance.toBuilder().transactionId(instance.getTransactionId() - 1).build();
        assertEquals(-1, instance2.compareTo(instance));
    }

    @Test
    public void testCompare_largerTransactionId_smallerPos() {
        TxOutput instance2 = instance.toBuilder().transactionId(instance.getTransactionId() + 1).pos((short) (instance.getPos() - 1)).build();
        assertEquals(1, instance2.compareTo(instance));
    }

    @Test
    public void testCompare_diff_addressId() {
        TxOutput instance2 = instance.toBuilder().addressId(instance.getAddressId() + 1).build();
        assertEquals(0, instance2.compareTo(instance));
    }
}
