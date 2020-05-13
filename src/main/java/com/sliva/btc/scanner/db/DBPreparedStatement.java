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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Sliva Co
 */
public class DBPreparedStatement {

    @Getter
    private final String query;
    private final int paramsCount;
    private final ThreadLocal<PreparedStatement> psPool;

    public DBPreparedStatement(String query, Supplier<Connection> conn) {
        checkArgument(query != null, "Argument 'query' is null");
        checkArgument(conn != null, "Argument 'conn' is null");
        this.query = query;
        this.paramsCount = StringUtils.countMatches(query, '?');
        this.psPool = ThreadLocal.withInitial(() -> buildPreparedStatement(conn));
    }

    public ParamSetter getParamSetter() {
        return new ParamSetter();
    }

    public <T> DBPreparedStatement setParameters(T element, BiConsumer<T, ParamSetter> fillCallback) {
        ParamSetter pSetter = getParamSetter();
        fillCallback.accept(element, pSetter);
        pSetter.checkStateReady();
        return this;
    }

    public DBPreparedStatement setParameters(Consumer<ParamSetter> fillCallback) {
        ParamSetter pSetter = getParamSetter();
        fillCallback.accept(pSetter);
        pSetter.checkStateReady();
        return this;
    }

    @SneakyThrows(SQLException.class)
    public DBPreparedStatement setMaxRows(int maxRowsLimit) {
        getPreparedStatement().setMaxRows(maxRowsLimit);
        return this;
    }

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

    @SneakyThrows(SQLException.class)
    public ResultSet executeQuery() {
        return getPreparedStatement().executeQuery();
    }

    @SneakyThrows(SQLException.class)
    public int executeQuery(QueryConsumer consumer) {
        AtomicInteger result = new AtomicInteger();
        try (ResultSet rs = executeQuery()) {
            while (rs.next()) {
                consumer.accept(rs);
                result.incrementAndGet();
            }
        }
        return result.get();
    }

    @SneakyThrows(SQLException.class)
    public <T> List<T> executeQueryToList(QueryResultProcessor<T> processor) {
        List<T> result = new ArrayList<>();
        try (ResultSet rs = executeQuery()) {
            while (rs.next()) {
                result.add(processor.apply(rs));
            }
        }
        return result;
    }

    /**
     * Query a single row and call processor with the ResultSet.
     *
     * @param <T> Value type returned by processor and passed through to return
     * @param processor Record processor to be called
     * @return Value returned by processor or empty if no record processed
     */
    @SneakyThrows(SQLException.class)
    public <T> Optional<T> querySingleRow(QueryResultProcessor<T> processor) {
        try (ResultSet rs = setMaxRows(1).executeQuery()) {
            if (rs.next()) {
                return Optional.ofNullable(processor.apply(rs));
            }
        }
        return Optional.empty();
    }

    private PreparedStatement getPreparedStatement() {
        return psPool.get();
    }

    @SneakyThrows(SQLException.class)
    private PreparedStatement buildPreparedStatement(Supplier<Connection> conn) {
        return conn.get().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public class ParamSetter {

        private final PreparedStatement ps = getPreparedStatement();
        private final AtomicInteger paramCounter = new AtomicInteger();

        private ParamSetter() {
        }

        public boolean isReady() {
            return paramCounter.get() == paramsCount;
        }

        public void checkStateReady() {
            checkState(isReady(), "Missing parameters. Defined: %s out of %s. Statement: \"%s\"", paramCounter.get(), paramsCount, query);
        }

        public void checkStateNotReady() {
            checkState(!isReady(), "No more statement parameters to set. Number of parameters: %s. Statement: \"%s\"", paramsCount, query);
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setString(String value) {
            checkStateNotReady();
            ps.setString(paramCounter.incrementAndGet(), value);
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setShort(short value) {
            checkStateNotReady();
            ps.setShort(paramCounter.incrementAndGet(), value);
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setInt(int value) {
            checkStateNotReady();
            ps.setInt(paramCounter.incrementAndGet(), value);
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setLong(long value) {
            checkStateNotReady();
            ps.setLong(paramCounter.incrementAndGet(), value);
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setBytes(byte[] value) {
            checkStateNotReady();
            ps.setBytes(paramCounter.incrementAndGet(), value);
            return this;
        }

        @SneakyThrows(SQLException.class)
        public ParamSetter setBoolean(boolean value) {
            checkStateNotReady();
            ps.setBoolean(paramCounter.incrementAndGet(), value);
            return this;
        }
    }

    public interface QueryConsumer {

        void accept(ResultSet rs) throws SQLException;
    }

    public interface QueryResultProcessor<T> {

        T apply(ResultSet rs) throws SQLException;
    }
}
