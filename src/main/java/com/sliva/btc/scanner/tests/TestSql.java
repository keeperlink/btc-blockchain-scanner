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
package com.sliva.btc.scanner.tests;

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.fasade.DbUpdateBlock;
import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.util.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class TestSql {

    private static DBConnectionSupplier conn;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        makeJDBCConnection();
        long s = System.currentTimeMillis();
        try (DbUpdateBlock addBlock = new DbUpdateBlock(conn)) {
            for (int i = 0; i < 10000; i++) {
                addBlock.add(BtcBlock.builder().height(i).hash(Utils.id2bin("000000000000000000119bbdfa591d9a3e932c2e0a8168eecc9fc1e0c8e11d1d")).txnCount(1).build());
            }
        }
        log.info("main(): runtime=" + (System.currentTimeMillis() - s) + " ms.");
    }

    private static void addBlock(int height, String hash) throws SQLException {
        String sql = "INSERT INTO block(height,hash)VALUES(?,?)";
        DBPreparedStatement ps = conn.prepareStatement(sql);
        ps.setParameters(p -> p.setInt(height).setBytes(Utils.id2bin(hash))).executeUpdate();
    }

    @SuppressWarnings("CallToPrintStackTrace")
    private static void makeJDBCConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            log.info("Congrats - Seems your MySQL JDBC Driver Registered!");
        } catch (ClassNotFoundException e) {
            log.info("Sorry, couldn't found JDBC driver. Make sure you have added JDBC Maven Dependency Correctly");
            e.printStackTrace();
            return;
        }

        try {
            // DriverManager: The basic service for managing a set of JDBC drivers.
            Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/btc?rewriteBatchedStatements=true", "root", "password");
            if (conn2 != null) {
                log.info("Connection Successful! Enjoy. Now it's time to push data");
            } else {
                log.info("Failed to make connection!");
            }
        } catch (SQLException e) {
            log.info("MySQL Connection Failed!");
            e.printStackTrace();
        }
    }
}
