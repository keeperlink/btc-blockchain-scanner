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

import com.sliva.btc.scanner.util.BitcoinPaymentURI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.BlockChainInfo;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.WalletInfo;

/**
 *
 * @author whost
 */
public class RpcClientTest {

    private final RpcClient instance = new RpcClient();

    public RpcClientTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        RpcSetup.init();
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
     * Test of getInstance method, of class RpcClient.
     */
    @Test
    public void testGetInstance() {
        System.out.println("getInstance");
        RpcClient result = RpcClient.getInstance();
        System.out.println("result=" + result);
        assertNotNull(result);
    }

    /**
     * Test of getClient method, of class RpcClient.
     */
    @Test
    public void testGetClient() {
        System.out.println("getClient");
        BitcoinJSONRPCClient result = instance.getClient();
        System.out.println("result=" + result);
        assertNotNull(result);
    }

    /**
     * Test of getBlock method, of class RpcClient.
     */
    @Test
    public void testGetBlock_int() {
        System.out.println("getBlock");
        int height = 123;
        BitcoindRpcClient.Block result = instance.getBlock(height);
        System.out.println("result=" + result);
        assertNotNull(result);
        assertEquals(height, result.height());
    }

    /**
     * Test of getBlock method, of class RpcClient.
     */
    @Test
    public void testGetBlock_String() {
        System.out.println("getBlock");
        String hash = "00000000a3bbe4fd1da16a29dbdaba01cc35d6fc74ee17f794cf3aab94f7aaa0";
        BitcoindRpcClient.Block result = instance.getBlock(hash);
        System.out.println("result=" + result);
        assertNotNull(result);
        assertEquals(hash, result.hash());
    }

    /**
     * Test of getRawTransaction method, of class RpcClient.
     */
    @Test
    public void testGetRawTransaction() {
        System.out.println("getRawTransaction");
        String txId = "b944ef8c77f9b5f4a4276880f17256988bba4d0125abc54391548061a688ae09";
        BitcoindRpcClient.RawTransaction result = instance.getRawTransaction(txId);
        System.out.println("result=" + result);
        assertNotNull(result);
        assertEquals(txId, result.hash());
    }

    /**
     * Test of getBlocksNumber method, of class RpcClient.
     */
    @Test
    public void testGetBlocksNumber() {
        System.out.println("getBlocksNumber");
        int result = instance.getBlocksNumber();
        System.out.println("result=" + result);
        assertTrue(result > 0);
    }

    @Test
    @Ignore
    public void testPayment() {
        System.out.println("testPayment");
        BitcoinJSONRPCClient client = instance.getClient();
        BlockChainInfo blockChainInfo = client.getBlockChainInfo();
        System.out.println("blockChainInfo=" + blockChainInfo);
        WalletInfo walletInfo = client.getWalletInfo();
        System.out.println("walletInfo=" + walletInfo);
        Map<String, Number> listAccounts = client.listAccounts();
        System.out.println("listAccounts=" + listAccounts);
        List<BitcoindRpcClient.ReceivedAddress> listReceivedByAddress = client.listReceivedByAddress();
        System.out.println("listReceivedByAddress=" + listReceivedByAddress);
        List<BitcoindRpcClient.Unspent> listUnspent = client.listUnspent();
        System.out.println("listUnspent=" + listUnspent);
        listUnspent.forEach(u -> System.out.println("\t" + u.txid() + ":" + u.vout() + " => " + u.address() + " (am:" + u.amount() + ", acc:" + u.account() + ")"));
        List<BitcoindRpcClient.Transaction> listTransactions = client.listTransactions();
        System.out.println("listTransactions=" + listTransactions);
        listTransactions.forEach(t -> System.out.println("\tdate=" + t.time() + ", txid=" + t.txId() + ", amount=" + t.amount()
                + ", address=" + t.address() + ", comment=" + t.comment() + ", commentTo=" + t.commentTo() + ", category=" + t.category()));
        List<String> addresses = client.getAddressesByAccount("");
        System.out.println("addresses=" + addresses);
//        String newAddress = client.getNewAddress();
//        System.out.println("newAddress=" + newAddress);
//        System.out.println("addresses=" + client.getAddressesByAccount(""));
//        for (int i = 1; i < 11; i++) {
//            BitcoindRpcClient.SmartFeeResult fee = client.getEstimateSmartFee(i);
//            System.out.println("blocks=" + fee.blocks() + ", est.fee=" + new DecimalFormat("#0.00000000").format(fee.feeRate()));
//        }
    }

    @Test
    @Ignore
    public void testQRCode() throws Exception {
        System.out.println("testQRCode");
        BitcoinJSONRPCClient client = instance.getClient();
        List<String> addresses = client.getAddressesByAccount("");
        String address = addresses.get(addresses.size() - 1);
        System.out.println("address=" + address);
        String qrText = new BitcoinPaymentURI.Builder().address(address).message("Test payment").label("test lbl").amount(0.001).build().getURI();
        //String qrText = "bitcoin:" + address + "?amount=0.001&message=Test%20Payment&label=test%20lbl&extra=other%20param";
        System.out.println("qrText=" + qrText);
        System.out.println("QR code URL: https://chart.googleapis.com/chart?chs=250x250&cht=qr&chl=" + BitcoinPaymentURI.encodeUrl(qrText));

//        QRCodeWriter qrCodeWriter = new QRCodeWriter();
//        BitMatrix bitMatrix = qrCodeWriter.encode(qrText, BarcodeFormat.QR_CODE, 350, 350);
//        Path path = FileSystems.getDefault().getPath("test-qr.png");
//        MatrixToImageWriter.writeToPath(bitMatrix, "PNG", path);
    }

    /**
     * Test of getBlockHash method, of class RpcClient.
     */
    @Test
    public void testGetBlockHash() {
        System.out.println("getBlockHash");
        int height = 123456;
        String expResult = "0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca";
        String result = instance.getBlockHash(height);
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

    /**
     * Test of getRawBlock method, of class RpcClient.
     */
    @Test
    public void testGetRawBlock_int() {
        System.out.println("getRawBlock");
        int height = 123456;
        String result = instance.getRawBlock(height);
        assertEquals(8358, result.length());
    }

    /**
     * Test of getRawBlock method, of class RpcClient.
     */
    @Test
    public void testGetRawBlock_String() {
        System.out.println("getRawBlock");
        String hash = "0000000000002917ed80650c6174aac8dfc46f5fe36480aaef682ff6cd83c3ca";
        String result = instance.getRawBlock(hash);
        assertEquals(8358, result.length());
    }

    @Test
    @Ignore
    public void testPerformance() throws InterruptedException {
        long s = System.currentTimeMillis();
        ExecutorService exec = Executors.newFixedThreadPool(20);
        for (int i = 500000; i < 500500; i++) {
            final int height = i;
            //exec.submit(() -> instance.getRawBlock(height));
            exec.submit(() -> instance.getClient().query("getblock", instance.getBlockHash(height), 0));
        }
        exec.shutdown();
        exec.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("Runtime: " + (System.currentTimeMillis() - s) + " ms.");
    }
}
