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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Sliva Co
 */
public class DBUtilsTest {

    public DBUtilsTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        DbSetup.init();
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
     * Test of readInteger method, of class DBUtils.
     */
    @Test
    public void test1() throws Exception {
        DBConnectionSupplier dbCon = new DBConnectionSupplier();
        System.out.println("dbName=" + dbCon.getDBName());
        dbCon.get().getMetaData();
        dbCon.get().prepareCall("SELECT 1").execute();
        StopWatch start = StopWatch.createStarted();
//        JdbcMetaDataCollector jmdc = new JdbcMetaDataCollector()
//                .setConnection(new DBConnectionSupplier().get())
//                .setParallelism(1);
//        com.afrunt.jdbcmetadata.JdbcDatabaseMetaData data = jmdc.collectDatabaseMetaData();//"btc3"::equalsIgnoreCase
//        System.out.println("data=" + data);
        DatabaseMetaData databaseMetaData = dbCon.getConnection().getMetaData();
        ResultSet rsTables = databaseMetaData.getTables("btc5", null, null, new String[]{"TABLE"});
        while (rsTables.next()) {
            //Print
            System.out.println(IntStream.rangeClosed(1, 10).mapToObj(n -> getString(n, rsTables)).collect(Collectors.joining("\t")));
        }
        ResultSet rsIndexes = databaseMetaData.getIndexInfo(dbCon.getDBName(), null, "input", false, false);
        while (rsIndexes.next()) {
            //Print
            System.out.println(IntStream.rangeClosed(1, 10).mapToObj(n -> getString(n, rsIndexes)).collect(Collectors.joining("\t")));
        }
        System.out.println("Runtime: " + Duration.ofNanos(start.getNanoTime()));
    }

    @SneakyThrows(SQLException.class)
    private String getString(int n, ResultSet resultSet) {
        return resultSet.getString(n);
    }
}