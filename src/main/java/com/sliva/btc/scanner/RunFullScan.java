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
package com.sliva.btc.scanner;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DbUpdate;
import com.sliva.btc.scanner.db.facade.DbCachedAddress;
import com.sliva.btc.scanner.db.facade.DbCachedAddressOne;
import com.sliva.btc.scanner.db.facade.DbCachedOutput;
import com.sliva.btc.scanner.db.facade.DbCachedTransaction;
import com.sliva.btc.scanner.db.facade.DbQueryBlock;
import com.sliva.btc.scanner.db.facade.DbQueryInput;
import com.sliva.btc.scanner.db.facade.DbQueryInputSpecial;
import com.sliva.btc.scanner.db.facade.DbQueryOutput;
import com.sliva.btc.scanner.db.facade.DbUpdateBlock;
import com.sliva.btc.scanner.db.facade.DbUpdateInput;
import com.sliva.btc.scanner.db.facade.DbUpdateInputSpecial;
import com.sliva.btc.scanner.db.facade.DbUpdateOutput;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.db.model.OutputStatus;
import com.sliva.btc.scanner.db.model.SighashType;
import com.sliva.btc.scanner.db.model.TXID;
import com.sliva.btc.scanner.db.model.TxInput;
import com.sliva.btc.scanner.db.model.TxInputSpecial;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.db.utils.DbValidationUtils;
import com.sliva.btc.scanner.rpc.RpcClient;
import com.sliva.btc.scanner.rpc.RpcClientDirect;
import com.sliva.btc.scanner.src.BJBlockProvider;
import com.sliva.btc.scanner.src.BlockProvider;
import com.sliva.btc.scanner.src.BlockProviderWithBackup;
import com.sliva.btc.scanner.src.RpcBlockProvider;
import com.sliva.btc.scanner.src.SrcBlock;
import com.sliva.btc.scanner.src.SrcTransaction;
import com.sliva.btc.scanner.util.BufferingAheadSupplier;
import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOption;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOptions;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.ShutdownHook;
import com.sliva.btc.scanner.util.Utils;
import static com.sliva.btc.scanner.util.Utils.getNumberSupplier;
import java.io.File;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunFullScan {

    private static final boolean DEFAULT_SAFE_RUN = false;
    private static final boolean DEFAULT_UPDATE_SPENT = true;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-scan-stop";
    private static final int DEFAULT_TXN_THREADS = 30;
    private static final int DEFAULT_PREFETCH_BUFFER_SIZE = 5;
    private static final int DEFAULT_LOAD_BLOCK_THREADS = 3;
    private static final int DEFAULT_PREPROC_BLOCK_THREADS = 3;

    private static final CmdOptions CMD_OPTS = new CmdOptions().add(DBConnectionSupplier.class)
            .add(DbUpdate.class).add(RpcClient.class).add(RpcClientDirect.class)
            .add(BJBlockProvider.class).add(DbValidationUtils.class)
            .add(DbCachedAddressOne.class).add(DbCachedOutput.class).add(DbCachedTransaction.class);
    private static final CmdOption safeRunOpt = buildOption(CMD_OPTS, null, "safe-run", true, "Run in safe mode - check DB for existing records before adding new. Default: " + DEFAULT_SAFE_RUN);
    private static final CmdOption updateSpentOpt = buildOption(CMD_OPTS, null, "update-spent", true, "Update spent flag on outpus. For better performance of massive update you might want to disable it and run separate process after this update is done. Default: " + DEFAULT_UPDATE_SPENT);
    private static final CmdOption blocksBackOpt = buildOption(CMD_OPTS, null, "blocks-back", true, "Check last number of blocks. Process will run in safe mode (option --safe-run=true)");
    private static final CmdOption startFromBlockOpt = buildOption(CMD_OPTS, null, "start-from-block", true, "Start checking from block hight provided. Process will run in safe mode (option --safe-run=true)");
    private static final CmdOption runToBlockOpt = buildOption(CMD_OPTS, null, "run-to-block", true, "Last block number to run");
    private static final CmdOption threadsOpt = buildOption(CMD_OPTS, null, "threads", true, "Number of threads to query DB. To disable parallel threading set value to 0. Default is " + DEFAULT_TXN_THREADS);
    private static final CmdOption stopFileOpt = buildOption(CMD_OPTS, null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end. Default: " + DEFAULT_STOP_FILE_NAME);
    private static final CmdOption loopOpt = buildOption(CMD_OPTS, null, "loop", true, "Repeat update every provided number of seconds");
    private static final CmdOption prefetchBufferSizeOpt = buildOption(CMD_OPTS, null, "prefetch-buffer-size", true, "Read ahead buffer size. Default: " + DEFAULT_PREFETCH_BUFFER_SIZE);
    private static final CmdOption loadBlockThreadsOpt = buildOption(CMD_OPTS, null, "load-block-threads", true, "Number of threads loading blocks. Default: " + DEFAULT_LOAD_BLOCK_THREADS);
    private static final CmdOption preprocBlockThreadsOpt = buildOption(CMD_OPTS, null, "preproc-block-threads", true, "Number of threads pre-processing blocks. Default: " + DEFAULT_PREPROC_BLOCK_THREADS);

    private static final AtomicBoolean terminateLoop = new AtomicBoolean();

    private final File stopFile;
    private final boolean safeRun;
    private final boolean runParallel;
    private final boolean updateSpent;
    private final ExecutorService execTxn;
    private final ExecutorService execInsOuts;
    private final DBConnectionSupplier dbCon;
    private final DbQueryBlock queryBlock;
    private final DbQueryInput queryInput;
    private final DbQueryInputSpecial queryInputSpecial;
    private final DbQueryOutput queryOutput;
    private final BlockProvider<?> blockProvider;
    private final Optional<Integer> startBlock;
    private final Optional<Integer> lastBlock;
    private final Optional<Integer> blocksBack;
    private final int nExecTxnThreads;
    private final int prefetchBufferSize;
    private final int loadBlockThreads;
    private final int preprocBlockThreads;
    private final LoadingCache<Integer, Collection<TxInput>> inputsCache;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    @SuppressWarnings({"SleepWhileInLoop"})
    public static void main(String[] args) throws Exception {
        CmdArguments cmd = CommandLineUtils.buildCmdArguments(args, Main.Command.update.name(), "Fill DB with block data retrieved from blockchain", null, CMD_OPTS);
        log.info("MAIN STARTED");
        ShutdownHook shutdownHook = new ShutdownHook(() -> terminateLoop.set(true));
        try {
            Thread.currentThread().setName("MAIN__________");
            Integer loopTime = cmd.getOption(loopOpt).map(Integer::parseInt).orElse(null);
            do {
                new RunFullScan(cmd).runProcess();
                if (loopTime != null && loopTime > 0 && !terminateLoop.get()) {
                    log.info("Execution finished. Sleeping for " + loopTime + " seconds...");
                    TimeUnit.SECONDS.sleep(loopTime);
                }
            } while (loopTime != null && !terminateLoop.get());
        } catch (Exception e) {
            log.error(null, e);
        } finally {
            log.info("MAIN FINISHED");
            shutdownHook.finished();
        }
    }

    public RunFullScan(CmdArguments cmd) throws Exception {
        dbCon = new DBConnectionSupplier().checkTablesExist("block", "transaction", "input", "input_special", "output", "address_p2pkh", "address_p2sh", "address_p2wpkh", "address_p2wsh");
        safeRun = cmd.getOption(safeRunOpt).map(Boolean::valueOf)
                .orElse(cmd.hasOption(startFromBlockOpt) || cmd.hasOption(blocksBackOpt) || DEFAULT_SAFE_RUN);
        startBlock = cmd.getOption(startFromBlockOpt).map(Integer::valueOf);
        lastBlock = cmd.getOption(runToBlockOpt).map(Integer::valueOf);
        blocksBack = cmd.getOption(blocksBackOpt).map(Integer::parseInt);
        updateSpent = dbCon.getDBMetaData().hasField("output.spent") && cmd.getOption(updateSpentOpt).map(Boolean::valueOf).orElse(DEFAULT_UPDATE_SPENT);
        stopFile = new File(cmd.getOption(stopFileOpt).orElse(DEFAULT_STOP_FILE_NAME));
        nExecTxnThreads = cmd.getOption(threadsOpt).map(Integer::parseInt).orElse(DEFAULT_TXN_THREADS);
        runParallel = nExecTxnThreads != 0;
        prefetchBufferSize = cmd.getOption(prefetchBufferSizeOpt).map(Integer::parseInt).orElse(DEFAULT_PREFETCH_BUFFER_SIZE);
        loadBlockThreads = cmd.getOption(loadBlockThreadsOpt).map(Integer::parseInt).orElse(DEFAULT_LOAD_BLOCK_THREADS);
        preprocBlockThreads = cmd.getOption(preprocBlockThreadsOpt).map(Integer::parseInt).orElse(DEFAULT_PREPROC_BLOCK_THREADS);
        execTxn = runParallel ? Executors.newFixedThreadPool(Math.max(1, nExecTxnThreads / 3),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecTxn-%02d").build()) : null;
        execInsOuts = runParallel ? Executors.newFixedThreadPool(Math.max(1, nExecTxnThreads * 2 / 3),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("execInsOuts-%02d").build()) : null;
        DbCachedAddressOne.CACHE_BY_ID_ENABLED = false;
        DbCachedTransaction.CACHE_BY_ID_ENABLED = false;
        queryBlock = new DbQueryBlock(dbCon);
        queryInput = new DbQueryInput(dbCon);
        queryInputSpecial = new DbQueryInputSpecial(dbCon);
        queryOutput = new DbQueryOutput(dbCon);
        if (cmd.hasOption(BJBlockProvider.fullBlocksPathOpt)) {
            blockProvider = new BlockProviderWithBackup(new BJBlockProvider(), new RpcBlockProvider());
        } else {
            blockProvider = new RpcBlockProvider();
        }
        inputsCache = !safeRun ? null : CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .maximumSize(200_000)
                .recordStats()
                .build(Utils.getCacheLoader(queryInput::findInputsByTransactionId));
    }

    public void runProcess() throws Exception {
        if (!safeRun) {
            DbValidationUtils.checkAndFixDataTails(dbCon);
            Utils.sleep(50);
        }
        boolean cacheOutput = safeRun || updateSpent;
        log.info("Execution STARTED");
        try (DbUpdateBlock addBlock = new DbUpdateBlock(dbCon);
                DbUpdateInput updateInput = new DbUpdateInput(dbCon);
                DbUpdateInputSpecial updateInputSpecial = new DbUpdateInputSpecial(dbCon);
                DbCachedTransaction cachedTxn = new DbCachedTransaction(dbCon);
                DbCachedAddress cachedAddress = new DbCachedAddress(dbCon);
                DbCachedOutput cachedOutput = cacheOutput ? new DbCachedOutput(dbCon) : null;
                DbUpdateOutput updateOutput = cacheOutput ? null : new DbUpdateOutput(dbCon)) {
            DbAccess db = new DbAccess(addBlock, updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput, updateOutput, queryOutput);
            int firstBlockToProcess = startBlock.orElseGet(() -> queryBlock.findLastHeight().orElse(-1) + 1 - blocksBack.orElse(0));
            int lastBlockToProcess = lastBlock.orElseGet(() -> new RpcClient().getBlocksNumber());
            log.info("firstBlockToProcess={}, lastBlockToProcess={}", firstBlockToProcess, lastBlockToProcess);

            ExecutorService loadThreadpool = Executors.newFixedThreadPool(loadBlockThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("loadBlock-%d").build());
            ExecutorService preprocThreadpool = Executors.newFixedThreadPool(preprocBlockThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("preprocBlock-%d").build());
            Supplier<Integer> blockNumberSupplier = getNumberSupplier(firstBlockToProcess, 1, n -> n <= lastBlockToProcess && !isTerminatingLoop());
            Supplier<CompletableFuture<SrcBlock<?>>> preProcFeatureSupplier
                    = () -> CompletableFuture
                            .completedFuture(blockNumberSupplier.get())
                            .thenApplyAsync(blockProvider::getBlock, loadThreadpool)
                            .thenApplyAsync(block -> preloadBlockCaches(block, db), preprocThreadpool);
            BufferingAheadSupplier<CompletableFuture<SrcBlock<?>>> bufferingSupplier
                    = new BufferingAheadSupplier<>(preProcFeatureSupplier, prefetchBufferSize);
            for (;;) {
                try {
                    bufferingSupplier.get().thenAccept(block -> processBlock(block, db)).get();
                } catch (NoSuchElementException ex) {
                    log.info("MAIN: No More Elements - Exiting the loop");
                    terminateLoop.set(true);
                    break;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            log.info("Execution FINISHED");
            DbUpdate.printStats();
        }
    }

    private void processBlock(SrcBlock<?> block, DbAccess db) {
        int blockHeight = block.getHeight();
        String blockHash = block.getHash();
        NumberFormat nf = NumberFormat.getIntegerInstance();
        log.info("Block({}).hash: {}, nTxns={}", nf.format(blockHeight), blockHash, nf.format(block.getTransactions().size()));
        if (!queryBlock.findBlockByHash(blockHash).isPresent()) {
            db.addBlock.add(BtcBlock.builder()
                    .height(blockHeight)
                    .hash(Utils.id2bin(blockHash))
                    .txnCount(block.getTransactions().size())
                    .build());
        }
        List<BtcTransaction> listTxn = safeRun ? db.cachedTxn.getTransactionsInBlock(blockHeight) : null;
        block.getTransactions().forEach(t -> processTransaction(t, blockHeight, listTxn, db));
        if (listTxn != null && !listTxn.isEmpty()) {
            log.debug("Found wrong transactions in block: " + listTxn);
            listTxn.forEach(db.cachedTxn::delete);
        }
        log.trace("processBlock({}): FINISHED", blockHeight);
    }

    private TxnProcessOutput processTransaction(int transactionId, DbAccess db) {
        BtcTransaction intx = db.cachedTxn.getTransaction(transactionId).orElseThrow(() -> new IllegalStateException("Transaction not found. transactionId=" + transactionId));
        log.debug("processTransaction({}): intx={}", transactionId, intx);
        List<BtcTransaction> listTxn = safeRun ? db.cachedTxn.getTransactionsInBlock(intx.getBlockHeight()) : null;
        return processTransaction(findBJTransaction(intx.getBlockHeight(), intx.getTxid().toString()), intx.getBlockHeight(), listTxn, db);
    }

    private TxnProcessOutput processTransaction(
            SrcTransaction<?, ?> t,
            int blockHeight, List<BtcTransaction> listTxn, DbAccess db) {
        String txHash = Utils.fixDupeTxid(t.getTxid(), blockHeight);
        log.trace("Tx.hash: {}", txHash);
        TXID txid = TXID.build(txHash);
        BtcTransaction btcTx = findTx(listTxn, txid).orElse(
                db.cachedTxn.add(BtcTransaction.builder()
                        .txid(txid.getData())
                        .blockHeight(blockHeight)
                        .nInputs(t.getInputs().size())
                        .nOutputs(t.getOutputs().size())
                        .build()));
        return TxnProcessOutput.builder()
                .tx(btcTx)
                .badInputs(processTransactionInputs(t, btcTx, db))
                .badOutputs(processTransactionOutputs(t, btcTx, db))
                .build();
    }

    @SuppressWarnings("UseSpecificCatch")
    private Collection<TxInput> processTransactionInputs(SrcTransaction<?, ?> t, BtcTransaction tx, DbAccess db) {
        final Collection<TxInput> txInputs;
        if (safeRun) {
            Collection<TxInput> c = inputsCache.getUnchecked(tx.getTransactionId());
            txInputs = c == null ? null : new ArrayList<>(c);
        } else {
            txInputs = null;
        }
        t.getInputs().forEach(ti -> {
            String inTxid = ti.getInTxid();
            final short inPos = ti.getInPos();
            log.trace("In.Outpoint: {}:{}", inTxid, inPos);
            try {
                final BtcTransaction inTxn = db.cachedTxn.getTransactionSimple(inTxid).orElseThrow(() -> new IllegalStateException("Transaction not found in DB: " + inTxid + " referenced from input#" + ti.getPos() + " in tx " + tx.toString()));
                TxInput inputToAdd = TxInput.builder()
                        .transactionId(tx.getTransactionId())
                        .pos(ti.getPos())
                        .inTransactionId(inTxn.getTransactionId())
                        .inPos(inPos)
                        .build();
                TxInput txInput = findInput(txInputs, ti.getPos()).orElse(null);
                if (txInput == null) {
                    if (safeRun) {
                        queryInput.findInputByOutTx(inTxn.getTransactionId(), inPos).ifPresent(in2 -> {
                            log.info("DB consistency issue: Found in DB input with same connected output:");
                            log.info("Adding     : " + frmtInput(inputToAdd, db));
                            log.info("Found in DB: " + frmtInput(in2, db));
                            if (!db.cachedTxn.getTransaction(in2.getTransactionId()).isPresent()) {
                                log.info("Transaction referenced from input does not exists: " + in2.getTransactionId());
                                db.updateInput.delete(in2);
                            } else {
                                processTransaction(in2.getTransactionId(), db);
                            }
                        });
                    }
                    db.updateInput.add(inputToAdd);
                } else {
                    if (txInput.getInTransactionId() != inputToAdd.getInTransactionId()
                            || txInput.getInPos() != inputToAdd.getInPos()) {
                        log.info("DB consistency issue: Found in DB input with different values:");
                        log.info("Validating : " + frmtInput(inputToAdd, db));
                        log.info("Found in DB: " + frmtInput(txInput, db));
                        if (txInput.getInTransactionId() != inputToAdd.getInTransactionId()
                                || txInput.getInPos() != inputToAdd.getInPos()) {
                            if (!db.cachedTxn.getTransaction(txInput.getInTransactionId()).isPresent()) {
                                log.info("Transaction referenced from input does not exists: " + txInput.getInTransactionId());
                                db.updateInput.update(inputToAdd);
                                db.updateInput.executeUpdates();
                            } else {
                                try {
                                    TxnProcessOutput out = processTransaction(txInput.getInTransactionId(), db);
                                    log.info("out={}", out);
                                } catch (Exception e) {
                                    log.debug(e.getMessage(), e);
                                }
                                db.updateInput.update(inputToAdd);
                                db.updateInput.executeUpdates();
                            }
                        }
                    }
                }
                if (ti.isMultisig() || ti.getSighashType() != SighashType.SIGHASH_ALL) {
                    //non-default SIGHASH_TYPE
                    TxInputSpecial newInputSpecial = TxInputSpecial.builder()
                            .transactionId(tx.getTransactionId())
                            .pos(ti.getPos())
                            .sighashType(ti.getSighashType())
                            .segwit(ti.isSegwit())
                            .multisig(ti.isMultisig())
                            .build();
                    if (safeRun) {
                        Optional<TxInputSpecial> c = queryInputSpecial.getInput(tx.getTransactionId(), ti.getPos());
                        if (!c.isPresent()) {
                            db.updateInputSpecial.add(newInputSpecial);
                        } else if (c.get().getSighashType() != newInputSpecial.getSighashType()
                                || c.get().isSegwit() != newInputSpecial.isSegwit()
                                || c.get().isMultisig() != newInputSpecial.isMultisig()) {
                            db.updateInputSpecial.update(newInputSpecial);
                        }
                    } else {
                        db.updateInputSpecial.add(newInputSpecial);
                    }
                }
                if (updateSpent) {
                    TxOutput txOutput = db.cachedOutput.getOutput(inTxn.getTransactionId(), inPos).orElseThrow(() -> new IllegalStateException("Output#" + ti.getPos() + " not found: " + inTxid + ":" + inPos + ". Src txn: " + tx.getTxid() + ". Ref tx: " + inTxn));
                    if (txOutput.getStatus() != OutputStatus.SPENT) {
                        db.cachedOutput.updateStatus(txOutput.getTransactionId(), txOutput.getPos(), OutputStatus.SPENT);
                    }
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        });
        if (txInputs != null) {
            txInputs.stream().forEach(txIn -> {
                log.debug("processTransactionInputs: Deleting record: " + txIn);
                db.updateInput.delete(txIn);
            });
        }
        return txInputs;
    }

    /**
     * Process Transaction outputs.
     *
     * @param t Transaction object
     * @param tx BtcTransaction object
     * @param cachedAddress DbCachedAddress
     * @param cachedOutput DbCachedOutput
     * @return List of transactions from DB that not part of Transaction
     * @throws SQLException
     */
    private Collection<TxOutput> processTransactionOutputs(SrcTransaction<?, ?> t, BtcTransaction tx, DbAccess db) {
        final Collection<TxOutput> txOutputs;
        if (safeRun) {
            //txOutputs = new ArrayList<>(db.cachedOutput.getOutputs(tx.getTransactionId()));
            txOutputs = t.getOutputs().stream().map(to -> db.cachedOutput.getOutput(tx.getTransactionId(), to.getPos())).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet());
        } else {
            txOutputs = null;
        }
        t.getOutputs().forEach(to -> {
            int addressId = to.getAddress().map(db.cachedAddress::getOrAdd).orElse(0);
            TxOutput txOutputToAdd = TxOutput.builder()
                    .transactionId(tx.getTransactionId())
                    .pos(to.getPos())
                    .amount(to.getValue())
                    .addressId(addressId)
                    .status(addressId == 0 ? OutputStatus.UNSPENDABLE : OutputStatus.UNSPENT)
                    .build();
            TxOutput txOutput = findOutput(txOutputs, txOutputToAdd.getPos()).orElse(null);
            if (txOutput == null) {
                if (db.cachedOutput != null) {
                    db.cachedOutput.add(txOutputToAdd);
                } else {
                    db.updateOutput.add(txOutputToAdd);
                }
            } else {
                if (txOutput.getAddressId() != txOutputToAdd.getAddressId()) {
                    log.debug("Address is different: {} <> {}({}) for output: {}. tx: {}", txOutput.getAddressId(), txOutputToAdd.getAddressId(), to.getAddress(), txOutputToAdd, tx);
                    db.cachedOutput.updateAddress(txOutputToAdd.getTransactionId(), txOutputToAdd.getPos(), txOutputToAdd.getAddressId());
                }
                if (txOutput.getAmount() != txOutputToAdd.getAmount()) {
                    log.debug("Amount is different: {} <> {} for output: {}. tx: {}", txOutput.getAmount(), txOutputToAdd.getAmount(), txOutputToAdd, tx);
                    db.cachedOutput.updateAmount(txOutputToAdd.getTransactionId(), txOutputToAdd.getPos(), txOutputToAdd.getAmount());
                }
            }
        });
        if (txOutputs != null) {
            txOutputs.forEach(txOut -> {
                log.debug("processTransactionOutputs: Deleting record: {}", txOut);
                if (db.cachedOutput != null) {
                    db.cachedOutput.delete(txOut);
                } else {
                    db.updateOutput.delete(txOut);
                }
            });
        }
        return txOutputs;
    }

    private SrcTransaction<?, ?> findBJTransaction(int blockHeight, String txid) {
        SrcBlock<?> block = blockProvider.getBlock(blockHeight);
        return findBJTransaction(block, blockHeight, txid);
    }

    private static SrcTransaction<?, ?> findBJTransaction(SrcBlock<?> block, int blockHeight, String txid) {
        return block.getTransactions().stream().filter((t) -> Utils.fixDupeTxid(t.getTxid(), blockHeight).equals(txid)).findAny().orElse(null);
    }

    private SrcBlock<?> preloadBlockCaches(SrcBlock<?> block, DbAccess db) {
        log.trace("preloadBlockCaches({}) STARTED", block.getHeight());
        try {
            block.getTransactions().stream()
                    .map(txn -> CompletableFuture.runAsync(() -> preProcTransaction(txn, block.getHeight(), db), execTxn))
                    .collect(Collectors.toList()).forEach(CompletableFuture::join);
            return block;
        } finally {
            log.trace("preloadBlockCaches({}) FINISHED", block.getHeight());
        }
    }

    private void preProcTransaction(SrcTransaction<?, ?> t, int blockHeight, DbAccess db) {
        long started = System.nanoTime();
        try {
            if (safeRun) {
                String txid = Utils.fixDupeTxid(t.getTxid(), blockHeight);
                db.cachedTxn.getTransactionSimple(txid).ifPresent(tx -> {
                    //db.cachedOutput.getOutputs(tx.getTransactionId());
                    t.getOutputs().forEach(to -> db.cachedOutput.getOutput(tx.getTransactionId(), to.getPos()));
                    inputsCache.getUnchecked(tx.getTransactionId());
                });
            }
            List<CompletableFuture<Void>> inFutures = t.getInputs().stream()
                    .map(ti -> CompletableFuture.runAsync(() -> db.cachedTxn.getTransactionSimple(ti.getInTxid())
                    .map(tr -> updateSpent ? db.cachedOutput.getOutput(tr.getTransactionId(), ti.getInPos()) : Optional.empty()), execInsOuts))
                    .collect(Collectors.toList());
            List<CompletableFuture<Void>> outFutures = t.getOutputs().stream()
                    .map(to -> CompletableFuture.runAsync(() -> to.getAddress().map(db.cachedAddress::getOrAdd), execInsOuts))
                    .collect(Collectors.toList());
            inFutures.forEach(CompletableFuture::join);
            outFutures.forEach(CompletableFuture::join);
        } finally {
            if (System.nanoTime() - started > 800_000_000) {
                log.trace("preProcTransaction: Long running transaction. txid={}, runtime={}", t.getTxid(), Duration.ofNanos(System.nanoTime() - started));
            }
        }
    }

    private boolean isTerminatingLoop() {
        if (!terminateLoop.get() && stopFile.exists()) {
            log.info("stopFile detected: {}", stopFile.getAbsolutePath());
            terminateLoop.set(true);
            stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
        }
        return terminateLoop.get();
    }

    private static String frmtInput(TxInput in, DbAccess db) {
        Optional<BtcTransaction> tx = db.cachedTxn.getTransaction(in.getTransactionId());
        Optional<BtcTransaction> intx = db.cachedTxn.getTransaction(in.getInTransactionId());
        Optional<TxOutput> out = db.cachedOutput != null ? db.cachedOutput.getOutput(in.getInTransactionId(), in.getInPos()) : db.queryOutput.getOutput(in.getInTransactionId(), in.getInPos());
        BtcAddress adr = out.flatMap(o -> db.cachedAddress.getAddress(o.getAddressId())).orElse(null);
        return "Input#" + in.getPos() + " in tx " + tx.map(BtcTransaction::getTxid).orElse(null)
                + " (" + tx.map(BtcTransaction::getBlockHeight).orElse(null) + "." + in.getTransactionId() + ")"
                + " pointing to output " + intx.map(BtcTransaction::getTxid).orElse(null) + ":" + in.getInPos()
                + " (" + intx.map(BtcTransaction::getBlockHeight).orElse(null) + "." + in.getInTransactionId() + ":" + in.getInPos() + ")"
                + " with address " + (adr == null ? null : adr.getBjAddress());

    }

    @NonNull
    private static Optional<BtcTransaction> findTx(Collection<BtcTransaction> list, TXID txid) {
        return list == null ? Optional.empty() : list.stream().filter(t -> t.getTxid().equals(txid)).peek(t -> list.remove(t)).findFirst();
    }

    @NonNull
    private static Optional<TxInput> findInput(Collection<TxInput> list, int pos) {
        return list == null ? Optional.empty() : list.stream().filter(t -> t.getPos() == pos).peek(t -> list.remove(t)).findFirst();
    }

    @NonNull
    private static Optional<TxOutput> findOutput(Collection<TxOutput> list, int pos) {
        return list == null ? Optional.empty() : list.stream().filter(t -> t.getPos() == pos).peek(t -> list.remove(t)).findFirst();
    }

    @AllArgsConstructor
    private static class DbAccess {

        private final DbUpdateBlock addBlock;
        private final DbUpdateInput updateInput;
        private final DbUpdateInputSpecial updateInputSpecial;
        private final DbCachedTransaction cachedTxn;
        private final DbCachedAddress cachedAddress;
        private final DbCachedOutput cachedOutput;
        private final DbUpdateOutput updateOutput;
        private final DbQueryOutput queryOutput;
    }

    @Getter
    @Builder
    @ToString
    private static class TxnProcessOutput {

        private final BtcTransaction tx;
        private final Collection<TxInput> badInputs;
        private final Collection<TxOutput> badOutputs;
    }
}
