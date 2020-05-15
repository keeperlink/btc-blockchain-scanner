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
import java.sql.Connection;
import java.sql.Statement;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
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
    private Connection connection;
    @Mock
    private Statement statement;
    //@Spy
    private DbCachedOutput instance;

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
        when(statement.execute(any(String.class))).thenReturn(Boolean.TRUE);
        //when(statement.executeUpdate(any(String.class))).thenReturn(1);
        when(connection.createStatement()).thenReturn(statement);
        when(dbConn.get()).thenReturn(connection);
        //when(dbConn.prepareStatement(any(String.class))).thenReturn(preparedStatement);
        when(dbConn.prepareStatement(any(String.class), any(String.class))).thenReturn(preparedStatement);
        when(dbConn.prepareStatement(any(String.class), any(String.class), any(String.class))).thenReturn(preparedStatement);
        instance = Mockito.spy(new DbCachedOutput(dbConn));
        FieldSetter.setField(instance, DbCachedOutput.class.getDeclaredField("queryOutput"), queryOutput);
        FieldSetter.setField(instance, DbCachedOutput.class.getDeclaredField("updateOutput"), updateOutput);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testAdd() {
        TxOutput txOutput = TxOutput.builder().transactionId(1).pos((short) 2).build();
        instance.add(txOutput);
        verify(updateOutput, only()).add(any());
        Optional<TxOutput> expected = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getIfPresentInCache(txOutput);
        System.out.println("result=" + result);
        verify(queryOutput, never()).getOutput(any());
        assertEquals(expected, result);
    }

    @Test
    public void testDelete() {
        TxOutput txOutput = TxOutput.builder().transactionId(1).pos((short) 2).build();
        when(updateOutput.delete(any(TxOutput.class))).thenReturn(Boolean.TRUE);
        instance.delete(txOutput);
        verify(updateOutput, only()).delete(any());
        assertEquals(Optional.empty(), instance.getIfPresentInCache(txOutput));
        instance.add(txOutput);
        Optional<TxOutput> expected = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getIfPresentInCache(txOutput);
        System.out.println("result=" + result);
        assertEquals(expected, result);
        instance.delete(txOutput);
        assertEquals(Optional.empty(), instance.getIfPresentInCache(txOutput));
    }

    @Test
    public void testUpdateStatus() {
        int transactionId = 1;
        short pos = 2;
        byte status = 3;
        TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
        instance.add(txOutput);
        instance.updateStatus(transactionId, pos, status);
        verify(updateOutput, times(1)).updateSpent(transactionId, pos, status);
        Optional<TxOutput> expected = Optional.of(txOutput.toBuilder().status(status).build());
        Optional<TxOutput> result = instance.getIfPresentInCache(txOutput);
        System.out.println("result=" + result);
        assertEquals(expected, result);
    }

    @Test
    public void testUpdateAddress() {
        int transactionId = 1;
        short pos = 2;
        int addressId = 3;
        TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
        instance.add(txOutput);
        instance.updateAddress(transactionId, pos, addressId);
        verify(updateOutput, times(1)).updateAddress(transactionId, pos, addressId);
        Optional<TxOutput> expected = Optional.of(txOutput.toBuilder().addressId(addressId).build());
        Optional<TxOutput> result = instance.getIfPresentInCache(txOutput);
        System.out.println("result=" + result);
        assertEquals(expected, result);
    }

    @Test
    public void testUpdateAmount() {
        int transactionId = 1;
        short pos = 2;
        long amount = 3L;
        TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
        instance.add(txOutput);
        instance.updateAmount(transactionId, pos, amount);
        verify(updateOutput, times(1)).updateAmount(transactionId, pos, amount);
        Optional<TxOutput> expected = Optional.of(txOutput.toBuilder().amount(amount).build());
        Optional<TxOutput> result = instance.getIfPresentInCache(txOutput);
        System.out.println("result=" + result);
        assertEquals(expected, result);
    }

    @Test
    public void testGetOutput_int_short() {
        int transactionId = 1;
        short pos = 2;
        TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
        instance.add(txOutput);
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getOutput(transactionId, pos);
        verify(queryOutput, never()).getOutput(any());
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetOutput_InOutKey() {
        int transactionId = 1;
        short pos = 2;
        TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
        InOutKey key = new InOutKey(transactionId, pos);
        instance.add(txOutput);
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getOutput(key);
        verify(queryOutput, never()).getOutput(any());
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetOutput_InOutKey_load() {
        int transactionId = 1;
        short pos = 2;
        TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
        InOutKey key = new InOutKey(transactionId, pos);
        when(queryOutput.getOutput(key)).thenReturn(Optional.of(txOutput));
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getOutput(key);
        verify(queryOutput, times(1)).getOutput(key);
        System.out.println("result=" + result);
        assertEquals(expResult, result);

        //second call must be cached
        result = instance.getOutput(key);
        verify(queryOutput, times(1)).getOutput(key);
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetOutput_InOutKey_not_found() {
        int transactionId = 1;
        short pos = 2;
        InOutKey key = new InOutKey(transactionId, pos);
        when(queryOutput.getOutput(key)).thenReturn(Optional.empty());
        Optional<TxOutput> expResult = Optional.empty();
        Optional<TxOutput> result = instance.getOutput(key);
        verify(queryOutput, times(1)).getOutput(key);
        System.out.println("result=" + result);
        assertEquals(expResult, result);

        //second call - cached .empty
        result = instance.getOutput(key);
        verify(queryOutput, times(1)).getOutput(key);
        System.out.println("result=" + result);
        assertEquals(expResult, result);

        //third call with diff key
        result = instance.getOutput(new InOutKey(transactionId + 1, pos));
        verify(queryOutput, times(2)).getOutput(any());
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

    @Test
    public void testClose() {
        instance.close();
    }

    /**
     * Test of getIfPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testGetIfPresentInCache() {
        System.out.println("getIfPresentInCache");
        int transactionId = 1;
        short pos = 2;
        InOutKey key = new InOutKey(transactionId, pos);

        Optional<TxOutput> expResult = Optional.empty();
        Optional<TxOutput> result = instance.getIfPresentInCache(key);
        System.out.println("result=" + result);
        assertEquals(expResult, result);
        verify(queryOutput, never()).getOutput(any());
    }

    /**
     * Test of getIfPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testGetIfPresentInCache_afterGet() {
        System.out.println("getIfPresentInCache");
        int transactionId = 1;
        short pos = 2;
        TxOutput txOutput = TxOutput.builder().transactionId(transactionId).pos(pos).build();
        InOutKey key = new InOutKey(transactionId, pos);
        when(queryOutput.getOutput(key)).thenReturn(Optional.of(txOutput));
        instance.getOutput(key);
        verify(queryOutput, times(1)).getOutput(any());
        Optional<TxOutput> expResult = Optional.of(txOutput);
        Optional<TxOutput> result = instance.getIfPresentInCache(key);
        assertEquals(expResult, result);
        verify(queryOutput, times(1)).getOutput(any());
    }

    /**
     * Test of getIfPresentInCache method, of class DbCachedOutput.
     */
    @Test
    public void testGetIfPresentInCache_afterGet_empty() {
        System.out.println("getIfPresentInCache");
        int transactionId = 1;
        short pos = 2;
        InOutKey key = new InOutKey(transactionId, pos);
        when(queryOutput.getOutput(key)).thenReturn(Optional.empty());
        instance.getOutput(key);
        verify(queryOutput, times(1)).getOutput(any());
        Optional<TxOutput> expResult = Optional.empty();
        Optional<TxOutput> result = instance.getIfPresentInCache(key);
        assertEquals(expResult, result);
        verify(queryOutput, times(1)).getOutput(any());
    }

}
