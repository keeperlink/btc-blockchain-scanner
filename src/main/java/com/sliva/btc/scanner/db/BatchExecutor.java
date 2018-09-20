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
package com.sliva.btc.scanner.db;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
@SuppressWarnings("UseSpecificCatch")
public final class BatchExecutor {

    public static <T> void executeBatch(Collection<T> list, PreparedStatement ps, FillStatement<T> fillCallback) {
        log.trace("BatchExecutor.executeBatch(): list.size={}", list.size());
        long s = System.currentTimeMillis();
        try {
            for (T a : list) {
                fillCallback.fill(a, ps);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            clearBatch(ps);
            list.forEach((a) -> {
                try {
                    fillCallback.fill(a, ps);
                    ps.execute();
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);
                    log.info("Exception while executing record insert: " + a);
                }
            });
        } finally {
            clearPsData(ps);
            log.debug("BatchExecutor.executeBatch({}): runtime={}", list.size(), (System.currentTimeMillis() - s) + " ms.");
        }
    }

    public static <T> void executeBatchFromFile(Collection<T> list, String tableName, DBConnection conn, BiConsumer<T, PrintStream> fillCallback)
            throws SQLException, IOException {
        log.trace("BatchExecutor.executeBatchFromFile(): list.size={}", list.size());
        String filename = "/temp/db-" + tableName + "-load-" + UUID.randomUUID() + ".data";
        File file = new File(filename);
        file.deleteOnExit();
        log.debug("executeBatchFromFile: temp file: {}", file.getAbsolutePath());
        long s = System.currentTimeMillis();
        try (PrintStream out = new PrintStream(file)) {
            for (T a : list) {
                fillCallback.accept(a, out);
            }
            out.close();
            conn.getConnection().prepareCall("LOAD DATA LOCAL INFILE '" + filename + "' INTO TABLE " + tableName).execute();
        } finally {
            log.debug("BatchExecutor.executeBatchFromFile({}): runtime={}", list.size(), (System.currentTimeMillis() - s) + " ms.");
        }
    }

    private static void clearBatch(PreparedStatement ps) {
        try {
            ps.clearBatch();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private static void clearPsData(PreparedStatement ps) {
        try {
            ps.clearParameters();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public interface FillStatement<T> {

        void fill(T t, PreparedStatement ps) throws SQLException;
    }
}
