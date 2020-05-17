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
package com.sliva.btc.scanner.db;

import com.sliva.btc.scanner.db.utils.DBMetaData;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Sliva Co
 */
public class DBMetaDataTest {

    private static DBConnectionSupplier dbCon;

    public DBMetaDataTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        DbSetup.init();
        dbCon = new DBConnectionSupplier();
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
     * Test of toString method, of class DBMetaData.
     */
    @Test
    public void testToString() {
        DBMetaData instance = new DBMetaData(dbCon);
        String result = instance.toString();
        System.out.println("result=" + result);
    }

}
