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
import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.db.utils.DBMetaData;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.MockitoJUnitRunner;

/**
 *
 * @author Sliva Co
 */
@RunWith(MockitoJUnitRunner.class)
public class DbCachedOutputTest {

    @Mock
    DBPreparedStatement preparedStatement;
    @Mock
    private DbQueryOutput queryOutput;
    @Mock
    private DbUpdateOutput updateOutput;
    @Mock
    private DBConnectionSupplier dbConn;
    @Mock
    private DBMetaData dbMetaData;
    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    //@Spy
    private DbCachedOutput instance;
    private DbUpdateOutput.CacheData updateOutputCacheData = new DbUpdateOutput.CacheData();

    private final int transactionId = 1;
    private final short pos = 2;
    private final InOutKey key = new InOutKey(transactionId, pos);
    private final TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
    private final int addressId = 3;
    private final long amount = 3L;
    private final byte status = 3;

    public DbCachedOutputTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws Exception {
        //when(preparedStatement.getParamSetter(any())).thenReturn(new DBPreparedStatement.ParamSetter());
        //when(preparedStatement.setParameters(any())).thenReturn(preparedStatement);
        //when(preparedStatement.setParameters(any(), any())).thenReturn(preparedStatement);
        //given(statement.execute(any(String.class))).willReturn(Boolean.TRUE);
        //when(statement.executeUpdate(any(String.class))).thenReturn(1);
        given(connection.createStatement()).willReturn(statement);
        given(dbConn.get()).willReturn(connection);
        given(dbMetaData.hasField(any())).willReturn(Boolean.TRUE);
        given(dbConn.getDBMetaData()).willReturn(dbMetaData);
        //when(dbConn.prepareStatement(any(String.class))).thenReturn(preparedStatement);
        given(dbConn.prepareStatement(any(), any())).willReturn(preparedStatement);
        given(updateOutput.getCacheData()).willReturn(updateOutputCacheData);
        given(updateOutput.isActive()).willReturn(Boolean.TRUE);
        instance = Mockito.spy(new DbCachedOutput(dbConn));
        FieldSetter.setField(instance, DbCachedOutput.class.getDeclaredField("queryOutput"), queryOutput);
        FieldSetter.setField(instance, DbCachedOutput.class.getDeclaredField("updateOutput"), updateOutput);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testAdd() {
        instance.add(txOutput);
        then(updateOutput).should().add(txOutput);
        assertInCache(txOutput, txOutput);
        then(queryOutput).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testDelete() {
        given(updateOutput.delete(any())).willReturn(Boolean.TRUE);
        instance.delete(txOutput);
        then(updateOutput).should().delete(txOutput);
        assertNotInCache(txOutput);

        instance.add(txOutput);
        assertInCache(txOutput, txOutput);
        instance.delete(txOutput);
        assertNotInCache(key);
    }

    @Test
    public void testDelete_removed_from_cache() {
        given(updateOutput.delete(any())).willReturn(Boolean.TRUE);
        instance.add(txOutput);
        instance.delete(txOutput);
        assertNotInCache(key);
    }

    @Test
    public void testUpdateStatus() {
        instance.updateStatus(transactionId, pos, status);
        then(updateOutput).should().updateSpent(transactionId, pos, status);
        assertNotInCache(txOutput);
    }

    @Test
    public void testUpdateStatus_cached() {
        instance.add(txOutput);
        instance.updateStatus(transactionId, pos, status);
        assertInCache(key, txOutput.toBuilder().status(status).build());
        then(updateOutput).should().updateSpent(transactionId, pos, status);
    }

    @Test
    public void testUpdateAddress() {
        instance.updateAddress(transactionId, pos, addressId);
        then(updateOutput).should().updateAddress(transactionId, pos, addressId);
        assertNotInCache(txOutput);
    }

    @Test
    public void testUpdateAddress_cached() {
        instance.add(txOutput);
        instance.updateAddress(transactionId, pos, addressId);
        assertInCache(txOutput, txOutput.toBuilder().addressId(addressId).build());
        then(updateOutput).should().updateAddress(transactionId, pos, addressId);
    }

    @Test
    public void testUpdateAmount() {
        instance.updateAmount(transactionId, pos, amount);
        then(updateOutput).should().updateAmount(transactionId, pos, amount);
        assertNotInCache(txOutput);
    }

    @Test
    public void testUpdateAmount_cached() {
        instance.add(txOutput);
        instance.updateAmount(transactionId, pos, amount);
        assertInCache(txOutput, txOutput.toBuilder().amount(amount).build());
        then(updateOutput).should().updateAmount(transactionId, pos, amount);
    }

    @Test
    public void testGetOutput_int_short() {
        instance.add(txOutput);
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getOutput(transactionId, pos);
        assertEquals(expResult, result);
        then(queryOutput).shouldHaveNoInteractions();
    }

    @Test
    public void testGetOutput_InOutKey() {
        instance.add(txOutput);
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getOutput(key);
        assertEquals(expResult, result);
        then(queryOutput).shouldHaveNoInteractions();
    }

    @Test
    public void testGetOutput_InOutKey_load() {
        given(queryOutput.getOutput(key)).willReturn(Optional.of(txOutput));
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getOutput(key);
        assertEquals(expResult, result);
        assertInCache(key, txOutput);
        then(queryOutput).should().getOutput(key);

        //second call must be cached
        result = instance.getOutput(key);
        assertEquals(expResult, result);
        then(queryOutput).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testGetOutput_InOutKey_not_found() {
        given(queryOutput.getOutput(key)).willReturn(Optional.empty());
        Optional<TxOutput> expResult = Optional.empty();
        Optional<TxOutput> result = instance.getOutput(key);
        assertEquals(expResult, result);
        assertInCache(key, null);
        then(queryOutput).should().getOutput(key);

        //second call - cached .empty
        result = instance.getOutput(key);
        assertEquals(expResult, result);
        then(queryOutput).shouldHaveNoMoreInteractions();

        //third call with diff key
        InOutKey key2 = new InOutKey(transactionId + 1, pos);
        given(queryOutput.getOutput(key2)).willReturn(Optional.empty());
        result = instance.getOutput(key2);
        assertEquals(expResult, result);
        then(queryOutput).should().getOutput(key2);
        then(queryOutput).shouldHaveNoMoreInteractions();
    }

    @Test
    public void testClose() {
        instance.add(txOutput);
        instance.close();
        assertNotInCache(key);
        then(updateOutput).should().close();
    }

    /**
     * Test of getIfPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testGetIfPresentInCache() {
        Optional<TxOutput> expResult = Optional.empty();
        Optional<TxOutput> result = instance.getIfPresentInCache(key);
        assertEquals(expResult, result);
        then(queryOutput).shouldHaveNoInteractions();
    }

    /**
     * Test of getIfPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testGetIfPresentInCache_afterGet() {
        given(queryOutput.getOutput(key)).willReturn(Optional.of(txOutput));
        instance.getOutput(key);
        then(queryOutput).should().getOutput(key);
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getIfPresentInCache(key);
        assertEquals(expResult, result);
        then(queryOutput).shouldHaveNoMoreInteractions();
    }

    /**
     * Test of getIfPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testGetIfPresentInCache_afterGet_empty() {
        given(queryOutput.getOutput(key)).willReturn(Optional.empty());
        instance.getOutput(key);
        then(queryOutput).should().getOutput(key);
        Optional<TxOutput> expResult = Optional.empty();
        Optional<TxOutput> result = instance.getIfPresentInCache(key);
        assertEquals(expResult, result);
        then(queryOutput).shouldHaveNoMoreInteractions();
    }

    /**
     * Test of isPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testIsPresentInCache() {
        boolean result = instance.isPresentInCache(key);
        assertFalse(result);
    }

    /**
     * Test of isPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testIsPresentInCache_true_after_add() {
        instance.add(txOutput);
        boolean result = instance.isPresentInCache(key);
        assertTrue(result);
    }

    /**
     * Test of isPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testIsPresentInCache_true_after_get() {
        given(queryOutput.getOutput(key)).willReturn(Optional.of(txOutput));
        instance.getOutput(key);
        boolean result = instance.isPresentInCache(key);
        assertTrue(result);
    }

    /**
     * Test of isPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testIsPresentInCache_true_after_get_empty() {
        given(queryOutput.getOutput(key)).willReturn(Optional.empty());
        instance.getOutput(key);
        boolean result = instance.isPresentInCache(key);
        assertTrue(result);
    }

    /**
     * Validate that value with provided key is present in cache.
     *
     * @param key Key
     * @param expected Expected value to be retrieved from cache, or null if
     * empty() value expected.
     */
    private void assertInCache(InOutKey key, TxOutput expected) {
        assertTrue(instance.isPresentInCache(key));
        assertEquals(Optional.ofNullable(expected), instance.getIfPresentInCache(key));
    }

    private void assertNotInCache(InOutKey key) {
        assertFalse(instance.isPresentInCache(key));
    }
}
