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
package com.sliva.btc.scanner.db.facade;

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.db.model.TXID;
import com.sliva.btc.scanner.db.utils.DBMetaData;
import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOptions;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildCmdArguments;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.MockitoJUnitRunner;

/**
 *
 * @author Sliva Co
 */
@RunWith(MockitoJUnitRunner.class)
public class DbCachedTransactionTest {

    @Mock
    DBPreparedStatement preparedStatement;
    @Mock
    private DbQueryTransaction queryTransaction;
    @Mock
    private DbUpdateTransaction updateTransaction;
    @Mock
    private DBConnectionSupplier dbConn;
    @Mock
    private DBMetaData dbMetaData;
    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    //@Spy
    private DbCachedTransaction instance;

    private final int blockHeight = 2;
    private final int transactionId = 1;
    private final TXID txid = new TXID(new byte[32]);
    private final String txHash = txid.toString();
    private final BtcTransaction btcTransaction = BtcTransaction.builder().txid(txid.getData()).blockHeight(blockHeight).build();
    private final BtcTransaction btcTransactionWithId = BtcTransaction.builder().transactionId(transactionId).txid(txid.getData()).blockHeight(blockHeight).build();

    public DbCachedTransactionTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws Exception {
//        given(statement.execute(any(String.class))).willReturn(Boolean.TRUE);
        given(connection.createStatement()).willReturn(statement);
        given(dbConn.get()).willReturn(connection);
//        given(dbMetaData.hasField(any())).willReturn(Boolean.TRUE);
//        given(dbConn.getDBMetaData()).willReturn(dbMetaData);
        given(dbConn.prepareStatement(any(), any())).willReturn(preparedStatement);
//        given(updateTransaction.getFromCache(any())).willReturn(null);
        given(updateTransaction.isActive()).willReturn(Boolean.TRUE);
        instance = Mockito.spy(new DbCachedTransaction(dbConn));
        FieldSetter.setField(instance, DbCachedTransaction.class.getDeclaredField("queryTransaction"), queryTransaction);
        FieldSetter.setField(instance, DbCachedTransaction.class.getDeclaredField("updateTransaction"), updateTransaction);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testApplyArguments() throws Exception {
        CommandLineUtils.CmdArguments cmdArguments = buildCmdArguments(new String[0], "", "", null, new CmdOptions());
        DbCachedTransaction.applyArguments(cmdArguments);
    }

    @Test
    public void testAdd() {
        BtcTransaction expResult = btcTransactionWithId;
        BtcTransaction result = instance.add(btcTransaction);
        assertEquals(expResult, result);
        then(updateTransaction).should().isActive();
        then(updateTransaction).should().add(btcTransactionWithId);
        then(updateTransaction).shouldHaveNoMoreInteractions();
        then(queryTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testAdd_dupe() {
        instance.add(btcTransaction);
        then(updateTransaction).should().add(btcTransactionWithId);
        assertEquals(Optional.of(btcTransactionWithId), instance.getIfPresentInCache(txid));

        //second add
        BtcTransaction expResult = btcTransaction;
        BtcTransaction result = instance.add(btcTransaction);
        assertEquals(expResult, result);
        then(updateTransaction).should(times(2)).isActive();
        then(updateTransaction).shouldHaveNoMoreInteractions();
        then(queryTransaction).shouldHaveNoInteractions();
        assertEquals(Optional.of(btcTransactionWithId), instance.getIfPresentInCache(txid));
    }

    @Test
    public void testDelete() {
        instance.delete(btcTransactionWithId);
        then(updateTransaction).should().isActive();
        then(updateTransaction).should().delete(btcTransactionWithId);
        then(updateTransaction).shouldHaveNoMoreInteractions();
        then(queryTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testDelete_was_present() {
        instance.add(btcTransaction);
        then(updateTransaction).should().add(btcTransactionWithId);

        instance.delete(btcTransactionWithId);
        assertFalse(instance.isPresentInCache(txid));
        then(updateTransaction).should(times(2)).isActive();
        then(updateTransaction).should().delete(btcTransactionWithId);
        then(updateTransaction).shouldHaveNoMoreInteractions();
        then(queryTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testGetTransaction_int() {
        given(queryTransaction.findTransaction(transactionId)).willReturn(Optional.of(btcTransactionWithId));
        Optional<BtcTransaction> expResult = Optional.of(btcTransactionWithId);
        Optional<BtcTransaction> result = instance.getTransaction(transactionId);
        assertEquals(expResult, result);
        then(queryTransaction).should().findTransaction(transactionId);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).should().getFromCache(transactionId);
        then(updateTransaction).shouldHaveNoMoreInteractions();
        assertEquals(Optional.of(btcTransactionWithId), instance.getIfPresentInCache(txid));
    }

    @Test
    public void testGetTransaction_int_empty() {
        given(queryTransaction.findTransaction(transactionId)).willReturn(Optional.empty());
        Optional<BtcTransaction> expResult = Optional.empty();
        Optional<BtcTransaction> result = instance.getTransaction(transactionId);
        assertEquals(expResult, result);
        then(queryTransaction).should().findTransaction(transactionId);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).should().getFromCache(transactionId);
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testGetTransaction_String() {
        given(queryTransaction.findTransaction(txid)).willReturn(Optional.of(btcTransactionWithId));
        Optional<BtcTransaction> expResult = Optional.of(btcTransactionWithId);
        Optional<BtcTransaction> result = instance.getTransaction(txHash);
        assertEquals(expResult, result);
        then(queryTransaction).should().findTransaction(txid);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).should().getFromCache(txid);
        then(updateTransaction).shouldHaveNoMoreInteractions();
        assertEquals(Optional.of(btcTransactionWithId), instance.getIfPresentInCache(txid));
    }

    @Test
    public void testGetTransaction_String_empty() {
        given(queryTransaction.findTransaction(txid)).willReturn(Optional.empty());
        Optional<BtcTransaction> expResult = Optional.empty();
        Optional<BtcTransaction> result = instance.getTransaction(txHash);
        assertEquals(expResult, result);
        then(queryTransaction).should().findTransaction(txid);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).should().getFromCache(txid);
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testGetTransactionSimple() {
        given(queryTransaction.findTransactionId(txid)).willReturn(Optional.of(transactionId));
        Optional<BtcTransaction> expResult = Optional.of(btcTransactionWithId.toBuilder().blockHeight(0).build());
        Optional<BtcTransaction> result = instance.getTransactionSimple(txHash);
        assertEquals(expResult, result);
        then(queryTransaction).should().findTransactionId(txid);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).should().getFromCache(txid);
        then(updateTransaction).shouldHaveNoMoreInteractions();
        assertEquals(expResult, instance.getIfPresentInCache(txid));
    }

    @Test
    public void testGetTransactionSimple_empty() {
        given(queryTransaction.findTransactionId(txid)).willReturn(Optional.empty());
        Optional<BtcTransaction> expResult = Optional.empty();
        Optional<BtcTransaction> result = instance.getTransactionSimple(txHash);
        assertEquals(expResult, result);
        then(queryTransaction).should().findTransactionId(txid);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).should().getFromCache(txid);
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testGetTransactionsInBlock() {
        given(queryTransaction.getTransactionsInBlock(blockHeight)).willReturn(Collections.singletonList(btcTransactionWithId));
        List<BtcTransaction> expResult = Collections.singletonList(btcTransactionWithId);
        List<BtcTransaction> result = instance.getTransactionsInBlock(blockHeight);
        assertEquals(expResult, result);
        then(queryTransaction).should().getTransactionsInBlock(blockHeight);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).shouldHaveNoInteractions();
        assertEquals(Optional.of(btcTransactionWithId), instance.getIfPresentInCache(txid));
    }

    @Test
    public void testGetTransactionsInBlock_empty() {
        given(queryTransaction.getTransactionsInBlock(blockHeight)).willReturn(Collections.emptyList());
        List<BtcTransaction> expResult = Collections.emptyList();
        List<BtcTransaction> result = instance.getTransactionsInBlock(blockHeight);
        assertEquals(expResult, result);
        then(queryTransaction).should().getTransactionsInBlock(blockHeight);
        then(queryTransaction).shouldHaveNoMoreInteractions();
        then(updateTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testGetIfPresentInCache_int() {
        Optional<BtcTransaction> expResult = Optional.empty();
        Optional<BtcTransaction> result = instance.getIfPresentInCache(transactionId);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testGetIfPresentInCache_int_present() {
        instance.add(btcTransactionWithId);
        then(updateTransaction).should().isActive();
        then(updateTransaction).should().add(any());
        Optional<BtcTransaction> expResult = Optional.of(btcTransactionWithId);
        Optional<BtcTransaction> result = instance.getIfPresentInCache(transactionId);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testGetIfPresentInCache_TXID() {
        Optional<BtcTransaction> expResult = Optional.empty();
        Optional<BtcTransaction> result = instance.getIfPresentInCache(txid);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testGetIfPresentInCache_TXID_present() {
        instance.add(btcTransactionWithId);
        then(updateTransaction).should().isActive();
        then(updateTransaction).should().add(any());
        Optional<BtcTransaction> expResult = Optional.of(btcTransactionWithId);
        Optional<BtcTransaction> result = instance.getIfPresentInCache(txid);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testGetIfPresentInCache_byteArr() {
        Optional<BtcTransaction> expResult = Optional.empty();
        Optional<BtcTransaction> result = instance.getIfPresentInCache(txid);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testGetIfPresentInCache_byteArr_present() {
        instance.add(btcTransactionWithId);
        then(updateTransaction).should().isActive();
        then(updateTransaction).should().add(any());
        Optional<BtcTransaction> expResult = Optional.of(btcTransactionWithId);
        Optional<BtcTransaction> result = instance.getIfPresentInCache(txid);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testIsPresentInCache_int() {
        boolean expResult = false;
        boolean result = instance.isPresentInCache(transactionId);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testIsPresentInCache_int_present() {
        instance.add(btcTransactionWithId);
        then(updateTransaction).should().isActive();
        then(updateTransaction).should().add(any());
        boolean expResult = true;
        boolean result = instance.isPresentInCache(transactionId);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testIsPresentInCache_TXID() {
        boolean expResult = false;
        boolean result = instance.isPresentInCache(txid);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoInteractions();
    }

    @Test
    public void testIsPresentInCache_TXID_present() {
        instance.add(btcTransactionWithId);
        then(updateTransaction).should().isActive();
        then(updateTransaction).should().add(any());
        boolean expResult = true;
        boolean result = instance.isPresentInCache(txid);
        assertEquals(expResult, result);
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testClose() {
        instance.close();
        then(queryTransaction).shouldHaveNoInteractions();
        then(updateTransaction).should().close();
        then(updateTransaction).shouldHaveNoMoreInteractions();
        then(queryTransaction).shouldHaveNoInteractions();
    }
}
