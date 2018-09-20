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

import com.sliva.btc.scanner.util.Utils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public abstract class DbUpdate implements AutoCloseable {

    private static final int MYSQL_BULK_INSERT_BUFFER_SIZE = 256 * 1024 * 1024;
    private static final int UPDATER_THREADS = 2;
    private static final ExecuteDbUpdate thread = new ExecuteDbUpdate();
    private static final Collection<DbUpdate> dbUpdateInstances = new ArrayList<>();
    private static final Set<DbUpdate> executingInstances = new HashSet<>();
    private static final ExecutorService executor = new ThreadPoolExecutor(UPDATER_THREADS, UPDATER_THREADS,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1));
    private static final Map<String, ExecStats> execStats = new HashMap<>();
    private static long startTimeMsec;
    private final DBConnection conn;
    private boolean isClosed = false;
    protected final Object execSync = new Object();

    static {
        thread.start();
    }

    @SuppressWarnings("LeakingThisInConstructor")
    public DbUpdate(DBConnection conn) {
        this.conn = conn;
        try {
            conn.getConnection().createStatement().execute("SET bulk_insert_buffer_size=" + MYSQL_BULK_INSERT_BUFFER_SIZE);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        synchronized (dbUpdateInstances) {
            dbUpdateInstances.add(this);
        }
    }

    protected DBConnection getConn() {
        return conn;
    }

    public boolean isIsClosed() {
        return isClosed;
    }

    public void setIsClosed(boolean isClosed) {
        this.isClosed = isClosed;
    }

    public void flushCache() {
        log.trace("flushCache() Called");
        DBUpdateCall c = new DBUpdateCall(this);
        while (c.call() != 0) {
        }
    }

    @Override
    public void close() {
        log.debug("{}.close()", this.getTableName());
        setIsClosed(true);
        flushCache();
        log.trace("{}.close() FINISHED", this.getTableName());
    }

    public abstract String getTableName();

    public abstract int getCacheFillPercent();

    public abstract boolean needExecuteInserts();

    public abstract int executeInserts();

    protected static void waitFullQueue(Collection queue, int maxQueueLength) {
        while (queue.size() >= maxQueueLength) {
            Utils.sleep(10);
        }
    }

    private static void updateRuntimeMap(String tableName, long records, long runtime) {
        synchronized (execStats) {
            ExecStats s = execStats.get(tableName);
            if (s == null) {
                execStats.put(tableName, s = new ExecStats());
            }
            s.addExecution(records, runtime);
        }
    }

    private static void printStats() {
        if (System.currentTimeMillis() - lastPrintedTime > 30 * 1000) {
            lastPrintedTime = System.currentTimeMillis();
            long runtimeInSec = Math.max(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTimeMsec), 1);
            synchronized (execStats) {
                execStats.entrySet().forEach((e) -> {
                    ExecStats s = e.getValue();
                    log.debug("{}\t Executions: {},\t Records: {},\t speed: {} rec/sec,\t runtime: {} sec.\t ({}%)",
                            StringUtils.rightPad(e.getKey(), 16),
                            s.getExecutions(),
                            s.getTotalRecords(),
                            s.getTotalRecords() / runtimeInSec,
                            TimeUnit.NANOSECONDS.toSeconds(s.getTotalRuntime()),
                            TimeUnit.NANOSECONDS.toSeconds(s.getTotalRuntime() * 100 / (runtimeInSec * UPDATER_THREADS))
                    );
                });
            }
        }
    }
    private static long lastPrintedTime = System.currentTimeMillis();

    private static final class ExecuteDbUpdate extends Thread {

        public ExecuteDbUpdate() {
            super("ExecuteDbUpdate");
        }

        @Override
        @SuppressWarnings({"SleepWhileInLoop"})
        public void run() {
            Utils.sleep(1000);
            log.info(getName() + ": STARTED");
            startTimeMsec = System.currentTimeMillis();
            try {
                for (;;) {
                    boolean executed = false;
                    try {
                        boolean allClosed = true;
                        int maxFillPercent = 0;
                        DbUpdate dbUpdateMaxFilled = null;
                        synchronized (dbUpdateInstances) {
                            for (DbUpdate dbUpdate : dbUpdateInstances) {
                                if (!dbUpdate.isClosed) {
                                    allClosed = false;
                                    if (!executingInstances.contains(dbUpdate) && dbUpdate.needExecuteInserts()) {
                                        int fillPercent = dbUpdate.getCacheFillPercent();
                                        if (fillPercent > maxFillPercent) {
                                            maxFillPercent = fillPercent;
                                            dbUpdateMaxFilled = dbUpdate;
                                        }
                                    }
                                }
                            }
                        }
                        if (allClosed && dbUpdateMaxFilled == null && executingInstances.isEmpty()) {
                            log.info("ExecuteDbUpdate: All updaters are closed - exiting this thread");
                            break;
                        }
                        if (dbUpdateMaxFilled != null) {
                            for (;;) {
                                try {
                                    synchronized (executingInstances) {
                                        log.trace("{}.Submitting ", dbUpdateMaxFilled.getClass().getSimpleName());
                                        executor.submit(new DBUpdateCall(dbUpdateMaxFilled));
                                        log.trace("{}.Submitted ", dbUpdateMaxFilled.getClass().getSimpleName());
                                        executingInstances.add(dbUpdateMaxFilled);
                                    }
                                    break;
                                } catch (RejectedExecutionException e) {
                                    Utils.sleep(500);
                                }
                            }
                            executed = true;
                        }
                        printStats();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    } finally {
                        if (!executed) {
                            Utils.sleep(10);
                        }
                    }
                }
            } finally {
                executor.shutdown();
                log.info(getName() + ": FINISHED");
            }
        }
    }

    @AllArgsConstructor
    @Slf4j
    private static class DBUpdateCall implements Callable<Integer> {

        private final DbUpdate dbUpdate;

        @Override
        @SuppressWarnings("UseSpecificCatch")
        public Integer call() {
            int nRecs = 0;
            try {
                log.trace("{}.executeInserts(): STARTED", dbUpdate.getTableName());
                long s = System.nanoTime();
                try {
                    nRecs = dbUpdate.executeInserts();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    long runtime = System.nanoTime() - s;
                    updateRuntimeMap(dbUpdate.getTableName(), nRecs, runtime);
                    if (nRecs > 0) {
                        log.debug("{}.executeInserts(): Records inserted: {} runtime {} ms.", dbUpdate.getTableName(), nRecs, TimeUnit.NANOSECONDS.toMillis(runtime));
                    }
                }
            } finally {
                synchronized (executingInstances) {
                    executingInstances.remove(dbUpdate);
                }
            }
            return nRecs;
        }
    }

    @Getter
    private static class ExecStats {

        private long executions;
        private long totalRecords;
        private long totalRuntime;

        void addExecution(long records, long runtime) {
            executions++;
            totalRecords += records;
            totalRuntime += runtime;
        }
    }
}
