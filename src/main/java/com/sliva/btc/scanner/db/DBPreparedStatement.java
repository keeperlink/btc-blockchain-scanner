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

import com.sliva.btc.scanner.db.utils.DbResultSetUtils;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import com.sliva.btc.scanner.db.utils.DbResultSetUtils.QueryConsumer;
import com.sliva.btc.scanner.db.utils.DbResultSetUtils.QueryResultProcessor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Sliva Co
 */
public class DBPreparedStatement {

    @Getter
    private final String query;
    @Getter
    private final int paramsCount;
    private final ThreadLocal<PreparedStatement> psPool;
    @Getter
    private final String cannotExecuteReason;

    /**
     * Create new instance of DBPreparedStatement.
     *
     * @param query SQL query string
     * @param conn DBConnectionSupplier object
     * @param cannotExecuteReason The reason why this query cannot be executed
     * or null if query can be executed.
     */
    public DBPreparedStatement(String query, DBConnectionSupplier conn, String cannotExecuteReason) {
        checkArgument(query != null, "Argument 'query' is null");
        checkArgument(conn != null, "Argument 'conn' is null");
        this.query = query;
        this.cannotExecuteReason = cannotExecuteReason;
        this.paramsCount = StringUtils.countMatches(query, '?');
        this.psPool = cannotExecuteReason != null ? null : ThreadLocal.withInitial(() -> buildPreparedStatement(conn));
    }

    /**
     * Return true if the statement can be executed, otherwise return false.
     *
     * @return true if the statement can be executed, otherwise return false
     */
    public boolean canExecute() {
        return cannotExecuteReason == null;
    }

    /**
     * Check if the statement can be executed, if not - throw
     * IlliegalStateException with the reason.
     */
    public void checkCanExecute() {
        checkState(cannotExecuteReason == null, "Cannot execute query. %s. Query: %s", cannotExecuteReason, query);
    }

    /**
     * Get parameters setter instance to be used to pass parameters to the
     * statement.
     *
     * @return ParamSetter instance
     */
    @NonNull
    public ParamSetter getParamSetter() {
        checkCanExecute();
        return new ParamSetter();
    }

    /**
     * Set statement parameters in batch mode.
     *
     * @param <T> Batch element type
     * @param element batch element
     * @param fillCallback Consumer with two arguments - batch element and
     * ParamSetter instance
     * @return this
     */
    @NonNull
    public <T> DBPreparedStatement setParameters(T element, BiConsumer<T, ParamSetter> fillCallback) {
        checkCanExecute();
        ParamSetter pSetter = getParamSetter();
        fillCallback.accept(element, pSetter);
        pSetter.checkStateReady();
        return this;
    }

    /**
     * Set statement parameters.
     *
     * @param fillCallback Consumer with ParamSetter instance
     * @return this
     */
    @NonNull
    public DBPreparedStatement setParameters(Consumer<ParamSetter> fillCallback) {
        checkCanExecute();
        ParamSetter pSetter = getParamSetter();
        fillCallback.accept(pSetter);
        pSetter.checkStateReady();
        return this;
    }

    /**
     * Set maximum number of rows to return.
     *
     * @param maxRowsLimit maximum number of rows to return
     * @return this
     */
    @NonNull
    @SneakyThrows(SQLException.class)
    public DBPreparedStatement setMaxRows(int maxRowsLimit) {
        getPreparedStatement().setMaxRows(maxRowsLimit);
        return this;
    }

    /**
     * Set fetch size.
     *
     * @param rows fetch size
     * @return this
     */
    @NonNull
    @SneakyThrows(SQLException.class)
    public DBPreparedStatement setFetchSize(int rows) {
        getPreparedStatement().setFetchSize(rows);
        return this;
    }

    @SneakyThrows(SQLException.class)
    public void addBatch() {
        getPreparedStatement().addBatch();
    }

    @SneakyThrows(SQLException.class)
    public void executeBatch() {
        getPreparedStatement().executeBatch();
    }

    @SneakyThrows(SQLException.class)
    public void clearBatch() {
        getPreparedStatement().clearBatch();
    }

    @SneakyThrows(SQLException.class)
    public void clearParameters() {
        getPreparedStatement().clearParameters();
    }

    @SneakyThrows(SQLException.class)
    public void execute() {
        getPreparedStatement().execute();
    }

    @SneakyThrows(SQLException.class)
    public int executeUpdate() {
        return getPreparedStatement().executeUpdate();
    }

    @NonNull
    @SneakyThrows(SQLException.class)
    public ResultSet executeQuery() {
        return getPreparedStatement().executeQuery();
    }

    public int executeQuery(QueryConsumer consumer) {
        return DbResultSetUtils.executeQuery(executeQuery(), consumer);
    }

    @NonNull
    public <T> List<T> executeQueryToList(QueryResultProcessor<T> processor) {
        return DbResultSetUtils.executeQueryToList(executeQuery(), processor);
    }

    /**
     * Query a single row and call processor with the ResultSet.
     *
     * @param <T> Value type returned by processor and passed through to return
     * @param processor Record processor to be called
     * @return Value returned by processor or empty if no record processed
     */
    @NonNull
    public <T> Optional<T> querySingleRow(QueryResultProcessor<T> processor) {
        return DbResultSetUtils.querySingleRow(setMaxRows(1).executeQuery(), processor);
    }

    @NonNull
    private PreparedStatement getPreparedStatement() {
        checkCanExecute();
        return psPool.get();
    }

    @NonNull
    @SneakyThrows(SQLException.class)
    private PreparedStatement buildPreparedStatement(Supplier<Connection> conn) {
        return conn.get().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public class ParamSetter {

        private final PreparedStatement ps = getPreparedStatement();
        private final AtomicInteger paramCounter = new AtomicInteger();
        private boolean ignoreExtraParam;

        private ParamSetter() {
        }

        public boolean isReady() {
            return paramCounter.get() == paramsCount;
        }

        public void checkStateReady() {
            checkState(isReady(), "Missing parameters. Defined: %s out of %s. Statement: \"%s\"", paramCounter.get(), paramsCount, query);
        }

        private boolean checkStateNotReady() {
            if (!ignoreExtraParam) {
                checkState(!isReady(), "No more statement parameters to set. Number of parameters: %s. Statement: \"%s\"", paramsCount, query);
            }
            return !isReady();
        }

        public int getParamsCount() {
            return paramsCount;
        }

        public ParamSetter ignoreExtraParam() {
            ignoreExtraParam = true;
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setString(String value) {
            if (checkStateNotReady()) {
                ps.setString(paramCounter.incrementAndGet(), value);
            }
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setShort(short value) {
            if (checkStateNotReady()) {
                ps.setShort(paramCounter.incrementAndGet(), value);
            }
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setInt(int value) {
            if (checkStateNotReady()) {
                ps.setInt(paramCounter.incrementAndGet(), value);
            }
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setLong(long value) {
            if (checkStateNotReady()) {
                ps.setLong(paramCounter.incrementAndGet(), value);
            }
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setBytes(byte[] value) {
            if (checkStateNotReady()) {
                ps.setBytes(paramCounter.incrementAndGet(), value);
            }
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setBoolean(boolean value) {
            if (checkStateNotReady()) {
                ps.setBoolean(paramCounter.incrementAndGet(), value);
            }
            return this;
        }
    }
}
