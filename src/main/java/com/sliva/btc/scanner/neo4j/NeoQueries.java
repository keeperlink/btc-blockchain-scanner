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

import com.sliva.btc.scanner.db.model.InOutKey;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.SummaryCounters;

/**
 * Neo4j queries. Not thread-safe.
 *
 * @author whost
 */
@Slf4j
public class NeoQueries implements AutoCloseable {

    private static final String QUERY_BATCH_UPLOAD = "UNWIND {param} AS tx"
            + " CREATE (t:Transaction{id:tx.id,hash:tx.hash,block:tx.block,nInputs:tx.nInputs,nOutputs:tx.nOutputs})"
            + " FOREACH (outp IN tx.outputs |"
            + " CREATE (o:Output{id:outp.id,address:outp.address,amount:outp.amount})<-[:output {pos:outp.pos}]-(t)"
            + " FOREACH (wal IN outp.wallets |"
            + " MERGE (w:Wallet{id:wal.id}) ON CREATE SET w.name=wal.name"
            + " CREATE (o)-[:wallet]->(w)))"
            + " WITH t, tx.inputs as inputs"
            + " UNWIND inputs AS inp"
            + " MATCH (o:Output {id:inp.id})"
            + " CREATE (o)-[oi:input {pos:inp.pos}]->(t)";
    private static final String QUERY_BATCH_UPLOAD_SAFE = "UNWIND {param} AS tx"
            + " MERGE (t:Transaction{id:tx.id}) ON CREATE SET t.hash=tx.hash,t.block=tx.block,t.nInputs=tx.nInputs,t.nOutputs=tx.nOutputs"
            + " FOREACH (outp IN tx.outputs |"
            + " MERGE (o:Output{id:outp.id}) ON CREATE SET o.address=outp.address,o.amount=outp.amount"
            + " MERGE (o)<-[:output {pos:outp.pos}]-(t)"
            + " FOREACH (wal IN outp.wallets |"
            + " MERGE (w:Wallet{id:wal.id}) ON CREATE SET w.name=wal.name"
            + " MERGE (o)-[:wallet]->(w)))"
            + " WITH t, tx.inputs as inputs"
            + " UNWIND inputs AS inp"
            + " MATCH (o:Output {id:inp.id})"
            + " MERGE (o)-[oi:input {pos:inp.pos}]->(t)";
    private static final String QUERY_UPDATE_WALLETS = "UNWIND {param} AS outp"
            + " MATCH (o:Output{id:outp.id})"
            + " FOREACH (wal IN outp.wallets |"
            + " MERGE (w:Wallet{id:wal.id}) ON CREATE SET w.name=wal.name"
            + " MERGE (o)-[:wallet]->(w))";
//            + " FOREACH (wal NOT IN outp.wallets |"
//            + " MERGE (w:Wallet{id:wal.id}) ON CREATE SET w.name=wal.name"
//            + " MERGE (o)-[:wallet]->(w))";
    private static final String QUERY_OUTPUTS_WITH_WALLETS = "MATCH (o:Output)-[p:wallet]->(w:Wallet)"
            + " WHERE o.id>={startOutputId} AND o.id<{endOutputId}"
            + " RETURN o.id,w.id,w.name";
    private static final String QUERY_UPDATE_OUTPUT_WALLET_RELATION
            = " MATCH (o:Output{id:{outputId}})"
            + " MERGE (w:Wallet{id:{walletId}}) ON CREATE SET w.name={walletName}"
            + " MERGE (o)-[:wallet]->(w)";
    private static final String QUERY_DELETE_OUTPUT_WALLET_RELATION
            = " MATCH (:Output{id:{outputId}})-[r:wallet]-(:Wallet{id:{walletId}})"
            + " DELETE r";

    private static final String QUERY_INPUT_RELATIONS_COUNTS = "MATCH (t:Transaction)-[p:input]-() RETURN count(p)";
    private static final String QUERY_OUTPUT_RELATIONS_COUNTS = "MATCH (t:Transaction)-[p:output]-() RETURN count(p)";
    private static final String QUERY_TRANSACTIONS_COUNTS = "MATCH (t:Transaction) RETURN COUNT(t),MAX(t.id),SUM(t.nInputs),SUM(t.nOutputs)";
    private static final String QUERY_TRANSACTIONS_MISSING_INPUT_RELATIONS = "MATCH (t:Transaction) WHERE t.nInputs>0 AND NOT (t)-[:input]-() RETURN t LIMIT 100";
    private static final String QUERY_TRANSACTIONS_MISSING_OUTPUT_RELATIONS = "MATCH (t:Transaction) WHERE NOT (t)-[:output]-() RETURN t LIMIT 100";
    private static final String QUERY_OUTPUTS_MISSING_OUTPUT_RELATIONS = "MATCH (o:Output) WHERE NOT (o)-[:output]-() RETURN o LIMIT 100";
    private static final String QUERY_OUTPUTS_MISSING_WALLET_RELATIONS = "MATCH (o:Output) WHERE (o)-[:input]->() AND NOT (o)-[:wallet]->() AND o.address<>'Undefined' RETURN o LIMIT 100";
    private static final String QUERY_WALLETS_WITHOUT_RELATIONS = "MATCH (w:Wallet) WHERE NOT (w)-[:wallet]-() RETURN w LIMIT 100";

    private final ThreadLocal<Session> session;

    public NeoQueries(NeoConnection conn) {
        this.session = ThreadLocal.withInitial(() -> conn.getSession());
    }

    public Session getSession() {
        return session.get();
    }

    public Transaction beginTransaction() {
        return getSession().beginTransaction();
    }

    public StatementResult run(String query) {
        return getSession().run(query);
    }

    public StatementResult run(String query, Value params) {
        return getSession().run(query, params);
    }

    public CompletionStage<StatementResultCursor> runAsync(String query) {
        return getSession().runAsync(query);
    }

    public String getImportDirectory() {
        StatementResult sr = run("call dbms.listConfig");
        while (sr.hasNext()) {
            Record r = sr.next();
            if ("dbms.directories.import".equals(r.get("name", ""))) {
                return r.get("value", (String) null);
            }
            log.trace("listConfig: {}", r.asMap());
        }
        return null;
    }

    public int getLastTransactionId() {
        //return getInteger("MATCH (n:Transaction) RETURN n.id ORDER BY n.id DESC LIMIT 1").orElse(0);
        //Neo4j currently having issues with using inexes for ORDER or MAX
        //do binary search as a workaround
        return findMax("MATCH (n:Transaction {id:{id}}) RETURN n.id");
    }

    public int findMax(String query) {
        int minVal = 0;
        int maxVal = Integer.MAX_VALUE;
        while (minVal < maxVal) {
            int middle = (int) ((1L + minVal + maxVal) / 2);
            OptionalInt o = NeoQueries.this.getResultAsInteger(query, Values.parameters("id", middle));
            log.trace("min:{}, max:{}, middle:{}, o:{}", minVal, maxVal, middle, o);
            if (o.isPresent()) {
                minVal = middle;
            } else {
                maxVal = middle - 1;
            }
        }
        return maxVal;
    }

    @NonNull
    public OptionalInt getResultAsInteger(String query) {
        Optional<Record> r = readFirstRecord(run(query));
        return r.isPresent() ? OptionalInt.of(r.get().get(0).asInt()) : OptionalInt.empty();
    }

    @NonNull
    public OptionalInt getResultAsInteger(String query, Value params) {
        Optional<Record> r = readFirstRecord(run(query, params));
        return r.isPresent() ? OptionalInt.of(r.get().get(0).asInt()) : OptionalInt.empty();
    }

    public void uploadBatch(PrepData data, boolean safeRun) {
        try (Transaction t = beginTransaction()) {
            StatementResult sr = t.run(safeRun ? QUERY_BATCH_UPLOAD_SAFE : QUERY_BATCH_UPLOAD, Values.parameters("param", data.data));
            t.commitAsync().thenRun(() -> log.trace("uploadBatch [{} - {}]: Transaction committed", data.startTransactionId, data.endTrasnactionId));
            logOutput(sr, "Output");
            sr.consume();
        }
    }

    public void updateWallets(Collection<Map<String, Object>> data, boolean safeRun) {
        try (Transaction t = beginTransaction()) {
            StatementResult sr = t.run(QUERY_UPDATE_WALLETS, Values.parameters("param", data));
            t.commitAsync().thenRun(() -> log.trace("updateWallets: Transaction committed"));
            logOutput(sr, "Output");
            sr.consume();
        }
    }

    public Collection<OutputWithWallet> queryOutputsWithWallets(int startTransactionId, int endTrasnactionId) {
        Collection<OutputWithWallet> result = new ArrayList<>();
        StatementResult sr = run(QUERY_OUTPUTS_WITH_WALLETS, Values.parameters(
                "startOutputId", toOutputId(startTransactionId, 0),
                "endOutputId", toOutputId(endTrasnactionId, 0)));
        while (sr.hasNext()) {
            Record r = sr.next();
            result.add(OutputWithWallet.builder()
                    .transactionId((int) (r.get(0).asLong() / 100_000))
                    .pos((short) (r.get(0).asLong() % 100_000))
                    .walletId(r.get(1).asInt())
                    .walletName(r.get(2).asString())
                    .build()
            );
        }
        return result;
    }

    public void updateOutputWalletRelation(int transactionId, int pos, int walletId, String walletName) {
        log.debug("updateOutputWalletRelation(transactionId:{},pos:{},walletId:{},walletName:{})", transactionId, pos, walletId, walletName);
        try (Transaction t = beginTransaction()) {
            StatementResult sr = t.run(QUERY_UPDATE_OUTPUT_WALLET_RELATION, Values.parameters("outputId", toOutputId(transactionId, pos), "walletId", walletId, "walletName", walletName));
            t.commitAsync().thenRun(() -> log.trace("updateOutputWalletRelation: Transaction committed"));
            logOutput(sr, "Output");
            sr.consume();
        }
    }

    public void deleteOutputWalletRelation(int transactionId, int pos, int walletId) {
        log.debug("deleteOutputWalletRelation(transactionId:{},pos:{},walletId:{})", transactionId, pos, walletId);
        try (Transaction t = beginTransaction()) {
            StatementResult sr = t.run(QUERY_DELETE_OUTPUT_WALLET_RELATION, Values.parameters("outputId", toOutputId(transactionId, pos), "walletId", walletId));
            t.commitAsync().thenRun(() -> log.trace("deleteOutputWalletRelation: Transaction committed"));
            logOutput(sr, "Output");
            sr.consume();
        }
    }

    @Getter
    @SuperBuilder(toBuilder = true)
    @ToString(callSuper = true)
    public static class OutputWithWallet extends InOutKey {

        private final int walletId;
        private final String walletName;
    }

    private static long toOutputId(int transactionId, int pos) {
        return transactionId * 100_000L + pos;
    }

    public void uploadFile(File f, boolean async, String query) {
        int fileDataLines;
        try {
            fileDataLines = FileUtils.readLines(f, StandardCharsets.ISO_8859_1).size() - 1;
        } catch (IOException e) {
            log.error("File: {}", f, e);
            throw new RuntimeException(e);
        }
        String txQuery = "USING PERIODIC COMMIT 1000000 LOAD CSV WITH HEADERS FROM \"file:/" + f.getName() + "\" as v" + " " + query;
        if (async) {
            long s = System.currentTimeMillis();
            runAsync(txQuery).thenAccept((c) -> {
                c.forEachAsync(r -> log.debug("fileDataLines: {}. Record: {}", fileDataLines, r.asMap())).thenAccept(sum -> log.debug("Records: {}. Summary: {}", fileDataLines, sum));
            }).thenRun(() -> {
                log.debug("{}. Runtime: {} msec.", f.getName(), System.currentTimeMillis() - s);
            });
        } else {
            Utils.logRuntime(f.getName(), () -> {
                StatementResult sr = run(txQuery);
                sr.list().forEach(r -> {
                    log.debug("fileDataLines: {}. Record: {}", fileDataLines, r.asMap());
                    int recordsUploaded = r.values().get(0).asInt();
                    if (recordsUploaded != fileDataLines) {
                        log.warn("Uploaded records do not match file({}) records count: {} <> {}", f, recordsUploaded, fileDataLines);
                    }
                });
                log.debug("Records: {}. Summary: {}", fileDataLines, toString(sr.summary().counters()));
            });
        }
    }

    public static Optional<Record> readFirstRecord(StatementResult sr) {
        try {
            return sr.hasNext() ? Optional.of(sr.next()) : Optional.empty();
        } finally {
            sr.consume();
        }
    }

    public static void logOutput(StatementResult sr, String logPrefix) {
        try {
            while (sr.hasNext()) {
                log.debug("{}: {}", logPrefix, sr.next().asMap());
            }
            log.debug("{}: Summary: {}", logPrefix, toString(sr.summary().counters()));
        } finally {
            sr.consume();
        }
    }

    private static String toString(SummaryCounters c) {
        return "nodesCreated:" + c.nodesCreated() + ", nodesDeleted:" + c.nodesDeleted()
                + ", relationshipsCreated:" + c.relationshipsCreated() + ", relationshipsDeleted:" + c.relationshipsDeleted()
                + ", propertiesSet:" + c.propertiesSet()
                + ", labelsAdded:" + c.labelsAdded() + ", labelsRemoved:" + c.labelsRemoved()
                + ", indexesAdded:" + c.indexesAdded() + ", indexesRemoved:" + c.indexesRemoved()
                + ", constraintsAdded:" + c.constraintsAdded() + ", constraintsRemoved:" + c.constraintsRemoved();
    }

    @Override
    public void close() throws Exception {
        session.get().close();
    }

    @Getter
    public static class PrepData {

        private final int startTransactionId;
        private final int endTrasnactionId;
        private final Collection<Map<String, Object>> data;

        public PrepData(int startTransactionId, int endTrasnactionId) {
            this.startTransactionId = startTransactionId;
            this.endTrasnactionId = endTrasnactionId;
            this.data = Collections.synchronizedCollection(new ArrayList<>(endTrasnactionId - startTransactionId + 1));
        }
    }

}
