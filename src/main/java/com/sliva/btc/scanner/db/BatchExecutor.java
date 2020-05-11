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

import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.db.DBPreparedStatement.ParamSetter;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

/**
 *
 * @author Sliva Co
 */
@Slf4j
@SuppressWarnings("UseSpecificCatch")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BatchExecutor {

    public static <T> void executeBatch(Collection<T> list, DBPreparedStatement ps, BiConsumer<T, ParamSetter> fillCallback) {
        StopWatch sw = StopWatch.createStarted();
        checkArgument(list != null, "Argument 'list' is null");
        checkArgument(ps != null, "Argument 'ps' is null");
        checkArgument(fillCallback != null, "Argument 'fillCallback' is null");
        log.trace("BatchExecutor.executeBatch(): list.size={}", list.size());
        try {
            list.forEach(a -> ps.setParameters(a, fillCallback).addBatch());
            ps.executeBatch();
        } catch (Exception e) {
            //if batch failed, then try to execute statements one by one
            log.error(e.getMessage(), e);
            clearBatchIgnoreExceptions(ps);
            list.forEach(a -> executeHandleExceptions(ps.setParameters(a, fillCallback),
                    ex -> log.error("Exception while executing query {} with parameters {}. Exception {}: {} ", ps.getQuery(), a, ex.getClass(), ex.getMessage())));
        } finally {
            clearPsDataIgnoreExceptions(ps);
            log.debug("BatchExecutor.executeBatch({}): runtime={}", list.size(), TimeUnit.NANOSECONDS.toMillis(sw.getNanoTime()) + " ms.");
        }
    }

    public static <T> void executeBatchFromFile(Collection<T> list, String tableName, DBConnectionSupplier conn, BiConsumer<T, PrintStream> fillCallback)
            throws SQLException, IOException {
        log.trace("BatchExecutor.executeBatchFromFile(): list.size={}", list.size());
        String filename = "/temp/db-" + tableName + "-load-" + UUID.randomUUID() + ".data";
        File file = new File(filename);
        file.deleteOnExit();
        log.debug("executeBatchFromFile: temp file: {}", file.getAbsolutePath());
        long s = System.currentTimeMillis();
        try (PrintStream out = new PrintStream(file)) {
            list.forEach(a -> fillCallback.accept(a, out));
            out.close();
            conn.get().prepareCall("LOAD DATA LOCAL INFILE '" + filename + "' INTO TABLE " + tableName).execute();
        } finally {
            log.debug("BatchExecutor.executeBatchFromFile({}): runtime={}", list.size(), (System.currentTimeMillis() - s) + " ms.");
        }
    }

    private static void executeHandleExceptions(DBPreparedStatement ps, Consumer<Exception> exceptionHandler) {
        try {
            ps.execute();
        } catch (Exception ex) {
            clearPsDataIgnoreExceptions(ps);
            if (exceptionHandler != null) {
                exceptionHandler.accept(ex);
            }
        }
    }

    private static void clearBatchIgnoreExceptions(DBPreparedStatement ps) {
        try {
            ps.clearBatch();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private static void clearPsDataIgnoreExceptions(DBPreparedStatement ps) {
        try {
            ps.clearParameters();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }
}
