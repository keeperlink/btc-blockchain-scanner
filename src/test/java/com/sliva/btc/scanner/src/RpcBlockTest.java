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

import com.sliva.btc.scanner.db.model.SighashType;
import com.sliva.btc.scanner.rpc.RpcClientDirect;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
public class RpcBlockTest {

    private static final String CONF_FILE = "/etc/rpc.conf";
    private RpcBlock instance = new RpcBlock("00000000000001991b57a7f352ebc922fce2f6874a97e43b30fce9f21a6d925f");

    public RpcBlockTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        init();
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

    public static void init() {
        try {
            CommandLine cmd = new DefaultParser().parse(RpcClientDirect.addOptions(new Options()), new String[]{"--rpc-config=" + CONF_FILE});
            RpcClientDirect.applyArguments(cmd);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test of getHash method, of class RpcBlock.
     */
    @Test
    public void testGetHash() {
        System.out.println("getHash");
        String expResult = "00000000000001991b57a7f352ebc922fce2f6874a97e43b30fce9f21a6d925f";
        String result = instance.getHash();
        assertEquals(expResult, result);
    }

    /**
     * Test of getHeight method, of class RpcBlock.
     */
    @Test
    public void testGetHeight() {
        System.out.println("getHeight");
        int expResult = 209079;
        int result = instance.getHeight();
        assertEquals(expResult, result);
    }

    /**
     * Test of getTransactions method, of class RpcBlock.
     */
    @Test
    public void testGetTransactions() {
        System.out.println("getTransactions");
        Stream<RpcTransaction> result = instance.getTransactions();
        assertEquals(528, result.count());
    }

    /**
     * Test of toString method, of class RpcBlock.
     */
    @Test
    public void testToString() {
        System.out.println("toString");
        String result = instance.toString();
        System.out.println("result=" + result.length());
    }

    /**
     * Test of getTransactions method, of class RpcBlock.
     */
    @Test
    public void testGetTransaction2() {
        System.out.println("testGetTransaction2");
        Stream<RpcTransaction> result = instance.getTransactions();
        RpcTransaction tx = result.filter(t -> "576600481ce4ff6626d2d5e79f73b9c2e03863b88f3fefe5036e90270b405b52".equals(t.getTxid())).findAny().get();
        RpcInput inp = tx.getInputs().findFirst().get();
        System.out.println("inp=" + inp);
        System.out.println("inp.signhashType=" +SighashType.toHexString(inp.getSighashType()));
    }

}
