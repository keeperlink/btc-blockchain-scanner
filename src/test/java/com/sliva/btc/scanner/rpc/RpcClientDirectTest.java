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
package com.sliva.btc.scanner.rpc;

import com.sliva.btc.scanner.util.BJBlockHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionWitness;
import org.bitcoinj.script.Script;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.spongycastle.util.encoders.Hex;

/**
 *
 * @author whost
 */
public class RpcClientDirectTest {

    private static final String CONF_FILE = "/etc/rpc.conf";
    private final RpcClientDirect instance = new RpcClientDirect();

    public RpcClientDirectTest() {
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
     * Test of applyArguments method, of class RpcClientDirect.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testApplyArguments() throws Exception {
        System.out.println("applyArguments");
        CommandLine cmd = new DefaultParser().parse(RpcClientDirect.addOptions(new Options()), new String[]{});
        RpcClientDirect.applyArguments(cmd);
    }

    /**
     * Test of addOptions method, of class RpcClientDirect.
     */
    @Test
    public void testAddOptions() {
        System.out.println("addOptions");
        Options options = new Options();
        Options result = RpcClientDirect.addOptions(options);
        assertTrue(result.hasOption("rpc-config"));
    }

    /**
     * Test of query method, of class RpcClientDirect.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testQuery() throws Exception {
        System.out.println("query");
        String method = "getblock";
        Object[] params = new Object[]{"0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca", 1};
        Object result = instance.query(method, params);
        System.out.println("result=" + result);
        assertNotNull(result);
    }

    /**
     * Test of getBlockHash method, of class RpcClientDirect.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetBlockHash() throws Exception {
        System.out.println("getBlockHash");
        int height = 123456;
        String expResult = "0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca";
        String result = instance.getBlockHash(height);
        assertEquals(expResult, result);
    }

    /**
     * Test of getRawBlock method, of class RpcClientDirect.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetRawBlock() throws Exception {
        System.out.println("getRawBlock");
        String hash = "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048";
        String expResult = "010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e362990101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000";
        String result = instance.getRawBlock(hash);
        System.out.println("result=" + result);
        assertNotNull(result);
        assertEquals(expResult, result);
        Block block = BJBlockHandler.parseBlcok(Hex.decode(result));
        System.out.println("block=" + block);
    }

    /**
     * Test of getRawBlock method, of class RpcClientDirect.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetRawBlock2() throws Exception {
        System.out.println("getRawBlock");
        String hash = "00000000000000000009f4678c46acd33844910f572af82eee3ca1204af9313f";
        String result = instance.getRawBlock(hash);
        System.out.println("result.size=" + result.length());
        assertNotNull(result);
        Block block = BJBlockHandler.parseBlcok(Hex.decode(result));
        System.out.println("block=" + block);
        Transaction tran = block.getTransactions().stream().filter(t -> "ed6ad1f896ff8221cfca585acf365c64b346dfc124fd9105de48e9fd22148d7b".equalsIgnoreCase(t.getHashAsString())).findAny().get();
        System.out.println("tran=" + tran);
        TransactionInput inp = tran.getInputs().get(0);
        System.out.println("inp=" + inp);
        System.out.println("inp.isOptInFullRBF=" + inp.isOptInFullRBF());
        Script scriptSig = inp.getScriptSig();
        System.out.println("scriptSig=" + scriptSig);
        TransactionWitness witness = inp.getWitness();
        System.out.println("witness=" + witness);
        System.out.println("witness.pushCount=" + witness.getPushCount());
    }

    /**
     * Test of getInstance method, of class RpcClientDirect.
     */
    @Test
    public void testGetInstance() {
        System.out.println("getInstance");
        RpcClientDirect result = RpcClientDirect.getInstance();
        assertNotNull(result);
    }

    /**
     * Test of getBlockHeight method, of class RpcClientDirect.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetBlockHeight() throws Exception {
        System.out.println("getBlockHeight");
        String hash = "0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca";
        int expResult = 123456;
        int result = instance.getBlockHeight(hash);
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

}
