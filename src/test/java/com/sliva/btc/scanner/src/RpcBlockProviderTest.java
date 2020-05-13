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
package com.sliva.btc.scanner.src;

import java.util.Collection;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author whost
 */
public class RpcBlockProviderTest {

    private final RpcBlockProvider instance = new RpcBlockProvider();

    public RpcBlockProviderTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        com.sliva.btc.scanner.rpc.RpcSetup.init();
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
     * Test of getBlock method, of class RpcBlockProvider.
     */
    @Test
    public void testGetBlock_int() {
        System.out.println("testGetBlock_int");
        int height = 123456;
        RpcBlock<?> result = instance.getBlock(height);
        assertEquals("0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca", result.getHash());
    }

    /**
     * Test of getBlock method, of class RpcBlockProvider.
     */
    @Test
    public void testGetBlock_String() {
        System.out.println("testGetBlock_String");
        String hash = "0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca";
        RpcBlock<?> result = instance.getBlock(hash);
        assertEquals(hash, result.getHash());
        assertEquals(123456, result.getHeight());
    }

    /**
     * Test of getBlock method, of class RpcBlockProvider.
     */
    @Test
    public void testGetBlock_transactions() {
        System.out.println("testGetBlock_transactions");
        int height = 1;
        RpcBlock<?> result = instance.getBlock(height);
        assertEquals("00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048", result.getHash());
        Collection<? extends RpcTransaction<?, ?>> tlist = result.getTransactions();
        assertNotNull(tlist);
        assertEquals(1, tlist.size());
        RpcTransaction<?, ?> t = tlist.iterator().next();
        assertNotNull(t.getInputs());
        assertTrue(t.getInputs().isEmpty());
        assertNotNull(t.getOutputs());
        assertEquals(1, t.getOutputs().size());
        RpcOutput<?> out = t.getOutputs().iterator().next();
        assertEquals("12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX", out.getAddress().map(RpcAddress::getName).orElse(null));
    }

    /**
     * Test of getBlock method, of class RpcBlockProvider.
     */
    @Test
    public void testGetAddress_OP_DUP() {
        System.out.println("testGetBlock_String");
        RpcBlock<?> block = instance.getBlock(150951);
        RpcTransaction<?, ?> tran = block.getTransactions().stream().filter(t -> t.getTxid().equalsIgnoreCase("07d33c8c74e945c50e45d3eaf4add7553534154503a478cf6d48e1c617b3f9f3")).findFirst().orElseThrow(() -> new IllegalStateException());
        assertEquals(Optional.empty(), tran.getOutputs().iterator().next().getAddress());
    }

    /**
     * Test of getBlock method, of class RpcBlockProvider.
     */
    @Test
    public void testGetBlock_transactions2() {
        System.out.println("testGetBlock_transactions2");
        int height = 123456;
        RpcBlock<?> result = instance.getBlock(height);
        assertEquals("0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca", result.getHash());
        Collection<? extends RpcTransaction<?, ?>> tlist = result.getTransactions();
        assertNotNull(tlist);
//        RpcTransaction t = tlist.get(10);
//        assertEquals("dda726e3dad9504dce5098dfab5064ecd4a7650bfe854bb2606da3152b60e427", t.getTxid());
//        assertNotNull(t.getInputs());
//        assertEquals(3, t.getInputs().size());
//        RpcInput in = t.getInputs().iterator().next();
//        assertEquals("ed7f7527ea6ad3975c7823f48136f74ecf54eccaa692178949a8f60c7e123c74", in.getInTxid());
//        assertNotNull(t.getOutputs());
//        assertEquals(2, t.getOutputs().size());
//        RpcOutput out = t.getOutputs().iterator().next();
//        assertEquals("15cKgW6QmdXBXMf7Q6twRPniFJZ863vwy6", out.getAddress().getName());
    }

    /**
     * Test of toString method, of class RpcBlockProvider.
     */
    @Test
    public void testToString() {
        System.out.println("toString");
        String result = instance.toString();
        System.out.println("result=" + result);
    }

}
