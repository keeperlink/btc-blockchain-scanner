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
package com.sliva.btc.scanner.neo4j;

import java.util.OptionalInt;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

/**
 *
 * @author whost
 */
//@Ignore
public class NeoQueriesTest {

    private static NeoConnection conn;
    private NeoQueries instance;

    public NeoQueriesTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        conn = new NeoConnection();
    }

    @AfterClass
    public static void tearDownClass() {
        conn.close();
    }

    @Before
    public void setUp() {
        instance = new NeoQueries(conn);
    }

    @After
    public void tearDown() throws Exception {
        instance.close();
    }

    /**
     * Test of getSession method, of class NeoQueries.
     */
    @Test
    public void testGetSession() {
        System.out.println("getSession");
        Session result = instance.getSession();
        System.out.println("result=" + result);
        assertNotNull(result);
    }

    /**
     * Test of run method, of class NeoQueries.
     */
    @Test
    public void testRun_String() {
        System.out.println("run");
        String query = "MATCH (n:Transaction {id:1}) RETURN n.id";
        Value expResult = Values.value(1);
        StatementResult result = instance.run(query);
        System.out.println("result=" + result);
        assertNotNull(result);
        assertTrue(result.hasNext());
        assertEquals(expResult, result.next().get(0));
        System.out.println("result=" + result.consume());
    }

    /**
     * Test of run method, of class NeoQueries.
     */
    @Test
    public void testRun_String_Value() {
        System.out.println("run");
        String query = "MATCH (n:Transaction {id:{myID}}) RETURN n.id";
        Value params = Values.parameters("myID", 1);
        Value expResult = Values.value(1);
        StatementResult result = instance.run(query, params);
        assertNotNull(result);
        assertTrue(result.hasNext());
        assertEquals(expResult, result.next().get(0));
    }

    /**
     * Test of runAsync method, of class NeoQueries.
     */
    @Test
    public void testRunAsync() {
    }

    /**
     * Test of getImportDirectory method, of class NeoQueries.
     */
    @Test
    public void testGetImportDirectory() {
        System.out.println("getImportDirectory");
        String result = instance.getImportDirectory();
        System.out.println("result=" + result);
        assertNotNull(result);
    }

    /**
     * Test of getLastTransactionId method, of class NeoQueries.
     */
    @Test
    public void testGetLastTransactionId() {
        System.out.println("getLastTransactionId");
        int result = instance.getLastTransactionId();
        System.out.println("result=" + result);
    }

    /**
     * Test of findMax method, of class NeoQueries.
     */
    @Test
    public void testFindMax() {
        System.out.println("findMax");
        String query = "MATCH (n:Transaction {id:{id}}) RETURN n.id";
        int result = instance.findMax(query);
        System.out.println("result=" + result);
    }

    /**
     * Test of getInteger method, of class NeoQueries.
     */
    @Test
    public void testGetInteger_String() {
        System.out.println("getInteger");
        String query = "MATCH (n:Transaction {id:1}) RETURN n.id";
        OptionalInt expResult = OptionalInt.of(1);
        OptionalInt result = instance.getInteger(query);
        assertEquals(expResult, result);
    }

    /**
     * Test of getInteger method, of class NeoQueries.
     */
    @Test
    public void testGetInteger_String_Value() {
        System.out.println("getInteger");
        String query = "MATCH (n:Transaction {id:{myID}}) RETURN n.id";
        Value params = Values.parameters("myID", 1);
        OptionalInt expResult = OptionalInt.of(1);
        OptionalInt result = instance.getInteger(query, params);
        assertEquals(expResult, result);
    }

    /**
     * Test of getInteger method, of class NeoQueries.
     */
    @Test
    public void testGetIntegerEmpty() {
        System.out.println("testGetIntegerEmpty");
        String query = "MATCH (n:Transaction {id:0}) RETURN n.id";
        OptionalInt expResult = OptionalInt.empty();
        OptionalInt result = instance.getInteger(query);
        System.out.println("result=" + result);
        assertEquals(expResult, result);
    }

    /**
     * Test of uploadFile method, of class NeoQueries.
     */
    @Test
    public void testUploadFile() {
    }

    /**
     * Test of close method, of class NeoQueries.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testClose() throws Exception {
    }

}
