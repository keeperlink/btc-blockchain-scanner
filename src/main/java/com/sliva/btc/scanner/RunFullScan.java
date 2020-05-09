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
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DbAddBlock;
import com.sliva.btc.scanner.db.DbCachedAddress;
import com.sliva.btc.scanner.db.DbCachedOutput;
import com.sliva.btc.scanner.db.DbCachedTransaction;
import com.sliva.btc.scanner.db.DbQueryBlock;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryInputSpecial;
import com.sliva.btc.scanner.db.DbUpdate;
import com.sliva.btc.scanner.db.DbUpdateAddressOne;
import com.sliva.btc.scanner.db.DbUpdateInput;
import com.sliva.btc.scanner.db.DbUpdateInputSpecial;
import com.sliva.btc.scanner.db.DbUpdateOutput;
import com.sliva.btc.scanner.db.DbUpdateTransaction;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.db.model.BtcBlock;
import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.db.model.OutputStatus;
import com.sliva.btc.scanner.db.model.SighashType;
import com.sliva.btc.scanner.db.model.TxInput;
import com.sliva.btc.scanner.db.model.TxInputSpecial;
import com.sliva.btc.scanner.db.model.TxOutput;
import com.sliva.btc.scanner.rpc.RpcClient;
import com.sliva.btc.scanner.rpc.RpcClientDirect;
import com.sliva.btc.scanner.src.BJBlockProvider;
import com.sliva.btc.scanner.src.BlockProvider;
import com.sliva.btc.scanner.src.BlockProviderWithBackup;
import com.sliva.btc.scanner.src.RpcBlockProvider;
import com.sliva.btc.scanner.src.SrcAddress;
import com.sliva.btc.scanner.src.SrcBlock;
import com.sliva.btc.scanner.src.SrcInput;
import com.sliva.btc.scanner.src.SrcOutput;
import com.sliva.btc.scanner.src.SrcTransaction;
import com.sliva.btc.scanner.util.BufferingAheadSupplier;
import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOption;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunFullScan {

    private static final boolean DEFAULT_SAFE_RUN = false;
    private static final int DEFAULT_BLOCKS_BACK = 0;
    private static final boolean DEFAULT_UPDATE_SPENT = true;
    private static final String DEFAULT_STOP_FILE_NAME = "/tmp/btc-scan-stop";
    private static final int DEFAULT_TXN_THREADS = 70;
    private static final int DEFAULT_PREFETCH_BUFFER_SIZE = 5;
    private static final int DEFAULT_LOAD_BLOCK_THREADS = 5;
    private static final int DEFAULT_PREPROC_BLOCK_THREADS = 3;

    public static final Collection<CmdOption> CMD_OPTS = new ArrayList<>();
    private static final CmdOption safeRunOpt = buildOption(CMD_OPTS, null, "safe-run", true, "Run in safe mode - check DB for existing records before adding new");
    private static final CmdOption updateSpentOpt = buildOption(CMD_OPTS, null, "update-spent", true, "Update spent flag on outpus. Default is true. For better performance of massive update you might want to disable it and run separate process after this update is done.");
    private static final CmdOption blocksBackOpt = buildOption(CMD_OPTS, null, "blocks-back", true, "Check last number of blocks. Process will run in safe mode (option --safe-run=true)");
    private static final CmdOption startFromBlockOpt = buildOption(CMD_OPTS, null, "start-from-block", true, "Start checking from block hight provided. Process will run in safe mode (option --safe-run=true)");
    private static final CmdOption runToBlockOpt = buildOption(CMD_OPTS, null, "run-to-block", true, "Last block number to run");
    private static final CmdOption threadsOpt = buildOption(CMD_OPTS, null, "threads", true, "Number of threads to query DB. Default is " + DEFAULT_TXN_THREADS + ". To disable parallel threading set value to 0");
    private static final CmdOption stopFileOpt = buildOption(CMD_OPTS, null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end.");
    private static final CmdOption loopOpt = buildOption(CMD_OPTS, null, "loop", true, "Repeat update every provided number of seconds");
    private static final CmdOption prefetchBufferSizeOpt = buildOption(CMD_OPTS, null, "prefetch-buffer-size", true, "Read ahead buffer size. Default: " + DEFAULT_PREFETCH_BUFFER_SIZE);
    private static final CmdOption loadBlockThreadsOpt = buildOption(CMD_OPTS, null, "load-block-threads", true, "Number of threads loading blocks. Default: " + DEFAULT_LOAD_BLOCK_THREADS);
    private static final CmdOption preprocBlockThreadsOpt = buildOption(CMD_OPTS, null, "preproc-block-threads", true, "Number of threads pre-processing blocks. Default: " + DEFAULT_PREPROC_BLOCK_THREADS);

    static {
        CMD_OPTS.addAll(DBConnectionSupplier.CMD_OPTS);
        CMD_OPTS.addAll(RpcClient.CMD_OPTS);
        CMD_OPTS.addAll(BJBlockProvider.CMD_OPTS);
    }

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
    private final BlockProvider blockProvider;
    private final Optional<Integer> startBlock;
    private final Optional<Integer> lastBlock;
    private final int blocksBack;
    private final int nExecTxnThreads;
    private final int prefetchBufferSize;
    private final int loadBlockThreads;
    private final int preprocBlockThreads;
    private final LoadingCache<Integer, Collection<TxInput>> inputsCache;
    private static boolean terminateLoop;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    @SuppressWarnings({"SleepWhileInLoop"})
    public static void main(String[] args) throws Exception {
        try {
            CmdArguments cmd = CommandLineUtils.buildCmdArguments(args, Main.Command.update.name(), CMD_OPTS);
            DBConnectionSupplier.applyArguments(cmd);
            BJBlockProvider.applyArguments(cmd);
            RpcClient.applyArguments(cmd);
            RpcClientDirect.applyArguments(cmd);
            Integer loopTime = cmd.getOption(loopOpt).map(Integer::parseInt).orElse(null);
            do {
                new RunFullScan(cmd).runProcess();
                if (loopTime != null && loopTime > 0 && !terminateLoop) {
                    log.info("Execution finished. Sleeping for " + loopTime + " seconds...");
                    TimeUnit.SECONDS.sleep(loopTime);
                }
            } while (loopTime != null && !terminateLoop);
        } catch (Exception e) {
            log.error(null, e);
        }
    }

    public RunFullScan(CmdArguments cmd) throws Exception {
        DbUpdateAddressOne.MAX_INSERT_QUEUE_LENGTH = 5000;
        DbUpdateInput.MAX_INSERT_QUEUE_LENGTH = 5000;
        DbUpdateInputSpecial.MAX_INSERT_QUEUE_LENGTH = 5000;
        DbUpdateOutput.MAX_INSERT_QUEUE_LENGTH = 5000;
        DbUpdateTransaction.MAX_INSERT_QUEUE_LENGTH = 5000;

        safeRun = cmd.getOption(safeRunOpt).map(Boolean::valueOf)
                .orElse(cmd.hasOption(startFromBlockOpt) || cmd.hasOption(blocksBackOpt) || DEFAULT_SAFE_RUN);
        startBlock = cmd.getOption(startFromBlockOpt).map(Integer::valueOf);
        lastBlock = cmd.getOption(runToBlockOpt).map(Integer::valueOf);
        blocksBack = cmd.getOption(blocksBackOpt).map(Integer::parseInt).orElse(DEFAULT_BLOCKS_BACK);
        updateSpent = cmd.getOption(updateSpentOpt).map(Boolean::valueOf).orElse(DEFAULT_UPDATE_SPENT);
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
        dbCon = new DBConnectionSupplier();
        queryBlock = new DbQueryBlock(dbCon);
        queryInput = new DbQueryInput(dbCon);
        queryInputSpecial = new DbQueryInputSpecial(dbCon);
        if (cmd.hasOption(BJBlockProvider.fullBlocksPathOpt)) {
            blockProvider = new BlockProviderWithBackup(new BJBlockProvider(), new RpcBlockProvider());
        } else {
            blockProvider = new RpcBlockProvider();
        }
        inputsCache = !safeRun ? null : CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .maximumSize(20_000)
                .recordStats()
                .build(
                        new CacheLoader<Integer, Collection<TxInput>>() {
                    @Override
                    public Collection<TxInput> load(Integer transactionId) throws SQLException {
                        return queryInput.getInputs(transactionId);
                    }
                });
    }

    public void runProcess() throws Exception {
        log.info("Execution STARTED");
        try (DbAddBlock addBlock = new DbAddBlock(dbCon);
                DbUpdateInput updateInput = new DbUpdateInput(dbCon);
                DbUpdateInputSpecial updateInputSpecial = new DbUpdateInputSpecial(dbCon);
                DbCachedTransaction cachedTxn = new DbCachedTransaction(dbCon);
                DbCachedAddress cachedAddress = new DbCachedAddress(dbCon);
                DbCachedOutput cachedOutput = new DbCachedOutput(dbCon)) {
            int firstBlockToProcess = startBlock.orElseGet(() -> getFirstUnprocessedBlockFromDB() - blocksBack);
            int lastBlockToProcess = lastBlock.orElseGet(() -> new RpcClient().getBlocksNumber());
            log.info("firstBlockToProcess={}, lastBlockToProcess={}", firstBlockToProcess, lastBlockToProcess);

            ExecutorService loadThreadpool = Executors.newFixedThreadPool(loadBlockThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("loadBlock-%d").build());
            ExecutorService preprocThreadpool = Executors.newFixedThreadPool(preprocBlockThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("preprocBlock-%d").build());
            Supplier<Integer> blockNumberSupplier = buildBlockNumberSupplier(firstBlockToProcess, lastBlockToProcess);
            Supplier<CompletableFuture<SrcBlock>> preProcFeatureSupplier
                    = () -> CompletableFuture
                            .completedFuture(blockNumberSupplier.get())
                            .thenApplyAsync(this::getBlock, loadThreadpool)
                            .thenApplyAsync(block -> preloadBlockCaches(block, cachedTxn, cachedOutput, cachedAddress), preprocThreadpool);
            BufferingAheadSupplier<CompletableFuture<SrcBlock>> bufferingSupplier = new BufferingAheadSupplier<>(preProcFeatureSupplier, prefetchBufferSize);
            for (;;) {
                try {
                    bufferingSupplier.get().thenAccept(block -> processBlock(block, addBlock, updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput)).join();
                } catch (NoSuchElementException ex) {
                    log.info("MAIN: No More Elements - Exiting the loop");
                    terminateLoop = true;
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

    private SrcBlock getBlock(int blockHeight) {
        log.trace("getBlock({}): STARTED", blockHeight);
        try {
            return blockProvider.getBlock(blockHeight);
        } finally {
            log.trace("getBlock({}): FINISHED", blockHeight);
        }
    }

    @SneakyThrows(SQLException.class)
    private void processBlock(
            SrcBlock<SrcTransaction> block,
            DbAddBlock addBlock,
            DbUpdateInput updateInput,
            DbUpdateInputSpecial updateInputSpecial,
            DbCachedTransaction cachedTxn,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) {
        int blockHeight = block.getHeight();
        String blockHash = block.getHash();
        log.info("Block({}).hash: {}, nTxns={}", blockHeight, blockHash, block.getTransactions().count());
        if (!queryBlock.findBlockByHash(blockHash).isPresent()) {
            addBlock.add(BtcBlock.builder()
                    .height(blockHeight)
                    .hash(blockHash)
                    .txnCount((int) block.getTransactions().count())
                    .build());
        }
        List<BtcTransaction> listTxn = safeRun ? cachedTxn.getTransactionsInBlock(blockHeight) : null;
        block.getTransactions().forEach(t -> processTransaction(t, blockHeight, listTxn, updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput));
        if (listTxn != null && !listTxn.isEmpty()) {
            log.debug("Found wrong transactions in block: " + listTxn);
            for (BtcTransaction t : listTxn) {
                cachedTxn.delete(t);
            }
        }
        log.trace("processBlock({}): FINISHED", blockHeight);
    }

    private Supplier<Integer> buildBlockNumberSupplier(int firstBlockToProcess, int lastBlockToProcess) {
        AtomicInteger currentBlock = new AtomicInteger(firstBlockToProcess);
        return () -> {
            synchronized (currentBlock) {
                if (currentBlock.get() > lastBlockToProcess || isTerminatingLoop()) {
                    throw new NoSuchElementException("No More Elements");
                }
                return currentBlock.getAndIncrement();
            }
        };
    }

    @SneakyThrows(SQLException.class)
    private int getFirstUnprocessedBlockFromDB() {
        return queryBlock.findLastHeight().orElse(-1) + 1;
    }

    private TxnProcessOutput processTransaction(
            int transactionId,
            DbUpdateInput updateInput,
            DbUpdateInputSpecial updateInputSpecial,
            DbCachedTransaction cachedTxn,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) throws SQLException, ExecutionException {
        BtcTransaction intx = cachedTxn.getTransaction(transactionId);
        log.debug("processTransaction({}): intx={}", transactionId, intx);
        List<BtcTransaction> listTxn = safeRun ? cachedTxn.getTransactionsInBlock(intx.getBlockHeight()) : null;
        try {
            return processTransaction(findBJTransaction(intx.getBlockHeight(), intx.getTxid()), intx.getBlockHeight(), listTxn, updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @SneakyThrows(SQLException.class)
    private TxnProcessOutput processTransaction(
            SrcTransaction t,
            int blockHeight,
            List<BtcTransaction> listTxn,
            DbUpdateInput updateInput,
            DbUpdateInputSpecial updateInputSpecial,
            DbCachedTransaction cachedTxn,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) {
        String txid = Utils.fixDupeTxid(t.getTxid(), blockHeight);
        log.trace("Tx.hash: {}", txid);
        BtcTransaction btcTx = findTx(listTxn, txid);
        if (btcTx == null) {
            btcTx = BtcTransaction.builder()
                    .txid(txid)
                    .blockHeight(blockHeight)
                    .nInputs(t.getInputs() == null ? 0 : (int) t.getInputs().count())
                    .nOutputs((int) t.getOutputs().count())
                    .build();
            btcTx = cachedTxn.add(btcTx);
        } else {
            //TODO validate
        }
        return TxnProcessOutput.builder()
                .tx(btcTx)
                .badInputs(processTransactionInputs(t, btcTx, updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput))
                .badOutputs(processTransactionOutputs(t, btcTx, cachedAddress, cachedOutput))
                .build();
    }

    @SuppressWarnings("UseSpecificCatch")
    @SneakyThrows({SQLException.class, ExecutionException.class})
    private Collection<TxInput> processTransactionInputs(
            SrcTransaction<SrcInput, SrcOutput<SrcAddress>> t,
            BtcTransaction tx,
            DbUpdateInput updateInput,
            DbUpdateInputSpecial updateInputSpecial,
            DbCachedTransaction cachedTxn,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) {
        if (t.getInputs() == null) {
            return null;
        }
        final Collection<TxInput> txInputs;
        if (safeRun) {
            Collection<TxInput> c = inputsCache.get(tx.getTransactionId());
            txInputs = c == null ? null : new ArrayList<>(c);
        } else {
            txInputs = null;
        }
        t.getInputs().forEach(ti -> {
            String inTxid = ti.getInTxid();
            final short inPos = ti.getInPos();
            log.trace("In.Outpoint: {}:{}", inTxid, inPos);
            try {
                final BtcTransaction inTxn = cachedTxn.getTransaction(inTxid);
                if (inTxn == null) {
                    throw new IllegalStateException("Transaction not found in DB: " + inTxid + " referenced from input#" + ti.getPos() + " in tx " + tx.toString());
                }
                TxInput txInput = findInput(txInputs, ti.getPos());
                TxOutput txOutput = cachedOutput.getOutput(inTxn.getTransactionId(), inPos).orElseThrow(() -> new IllegalStateException("Output#" + ti.getPos() + " not found: " + inTxid + ":" + inPos + ". Src txn: " + tx.getTxid() + ". Ref tx: " + inTxn));
                TxInput inputToAdd = TxInput.builder()
                        .transactionId(tx.getTransactionId())
                        .pos(ti.getPos())
                        .inTransactionId(inTxn.getTransactionId())
                        .inPos(inPos)
                        .build();
                if (txInput == null) {
                    if (safeRun) {
                        TxInput in2 = queryInput.findInputByOutTx(inTxn.getTransactionId(), inPos);
                        if (in2 != null) {
                            log.info("DB consistency issue: Found in DB input with same connected output:");
                            log.info("Adding     : " + frmtInput(inputToAdd, cachedTxn, cachedOutput, cachedAddress));
                            log.info("Found in DB: " + frmtInput(in2, cachedTxn, cachedOutput, cachedAddress));
                            BtcTransaction intx = cachedTxn.getTransaction(in2.getTransactionId());
                            if (intx == null) {
                                log.info("Transaction referenced from input does not exists: " + in2.getTransactionId());
                                updateInput.delete(in2);
                            } else {
                                processTransaction(in2.getTransactionId(), updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput);
                            }
                        }
                    }
                    updateInput.add(inputToAdd);
                } else {
                    if (txInput.getInTransactionId() != inputToAdd.getInTransactionId()
                            || txInput.getInPos() != inputToAdd.getInPos()) {
                        log.info("DB consistency issue: Found in DB input with different values:");
                        log.info("Validating : " + frmtInput(inputToAdd, cachedTxn, cachedOutput, cachedAddress));
                        log.info("Found in DB: " + frmtInput(txInput, cachedTxn, cachedOutput, cachedAddress));
                        if (txInput.getInTransactionId() != inputToAdd.getInTransactionId()
                                || txInput.getInPos() != inputToAdd.getInPos()) {
                            BtcTransaction intx = cachedTxn.getTransaction(txInput.getInTransactionId());
                            if (intx == null) {
                                log.info("Transaction referenced from input does not exists: " + txInput.getInTransactionId());
                                updateInput.update(inputToAdd);
                                updateInput.executeUpdates();
                            } else {
                                try {
                                    TxnProcessOutput out = processTransaction(txInput.getInTransactionId(), updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput);
                                    log.info("out={}", out);
                                } catch (Exception e) {
                                    log.debug(e.getMessage(), e);
                                }
                                updateInput.update(inputToAdd);
                                updateInput.executeUpdates();
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
                        TxInputSpecial c = queryInputSpecial.getInput(tx.getTransactionId(), ti.getPos());
                        if (c == null) {
                            updateInputSpecial.add(newInputSpecial);
                        } else if (c.getSighashType() != newInputSpecial.getSighashType()
                                || c.isSegwit() != newInputSpecial.isSegwit()
                                || c.isMultisig() != newInputSpecial.isMultisig()) {
                            updateInputSpecial.update(newInputSpecial);
                        }
                    } else {
                        updateInputSpecial.add(newInputSpecial);
                    }
                }
                if (updateSpent && txOutput.getStatus() != OutputStatus.SPENT) {
                    cachedOutput.updateStatus(txOutput.getTransactionId(), txOutput.getPos(), OutputStatus.SPENT);
                }
            } catch (SQLException | ExecutionException e) {
                log.error(e.getMessage(), e);
            }
        });
        if (txInputs != null) {
            for (TxInput txIn : txInputs) {
                log.debug("processTransactionInputs: Deleting record: " + txIn);
                updateInput.delete(txIn);
            }
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
    @SneakyThrows(SQLException.class)
    private Collection<TxOutput> processTransactionOutputs(
            SrcTransaction<SrcInput, SrcOutput<SrcAddress>> t,
            BtcTransaction tx,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) {
        final Collection<TxOutput> txOutputs;
        if (safeRun) {
            Collection<TxOutput> c = cachedOutput.getOutputs(tx.getTransactionId());
            txOutputs = c == null ? null : new ArrayList<>(c);
        } else {
            txOutputs = null;
        }
        t.getOutputs().forEach(to -> {
            SrcAddress addr = to.getAddress();
            try {
                int addressId = addr == null ? 0 : cachedAddress.getOrAdd(addr, false);
                TxOutput txOutputToAdd = TxOutput.builder()
                        .transactionId(tx.getTransactionId())
                        .pos(to.getPos())
                        .amount(to.getValue())
                        .addressId(addressId)
                        .status(OutputStatus.UNDEFINED)
                        .build();
                TxOutput txOutput = findOutput(txOutputs, txOutputToAdd.getPos());
                if (txOutput == null) {
                    cachedOutput.add(txOutputToAdd);
                } else {
                    try {
                        if (txOutput.getAddressId() != txOutputToAdd.getAddressId()) {
                            log.debug("Address is different: {} <> {}({}) for output: {}. tx: {}", txOutput.getAddressId(), txOutputToAdd.getAddressId(), addr, txOutputToAdd, tx);
                            cachedOutput.updateAddress(txOutputToAdd.getTransactionId(), txOutputToAdd.getPos(), txOutputToAdd.getAddressId());
                        }
                        if (txOutput.getAmount() != txOutputToAdd.getAmount()) {
                            log.debug("Amount is different: {} <> {} for output: {}. tx: {}", txOutput.getAmount(), txOutputToAdd.getAmount(), txOutputToAdd, tx);
                            cachedOutput.updateAmount(txOutputToAdd.getTransactionId(), txOutputToAdd.getPos(), txOutputToAdd.getAmount());
                        }
                    } catch (SQLException e) {
                        log.debug(e.getMessage(), e);
                    }
                }
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        });
        if (txOutputs != null) {
            for (TxOutput txOut : txOutputs) {
                log.debug("processTransactionOutputs: Deleting record: " + txOut);
                cachedOutput.delete(txOut);
            }
        }
        return txOutputs;
    }

    private SrcTransaction findBJTransaction(int blockHeight, String txid) throws SQLException, IOException {
        SrcBlock block = blockProvider.getBlock(blockHeight);
        return findBJTransaction(block, blockHeight, txid);
    }

    private static SrcTransaction findBJTransaction(SrcBlock<SrcTransaction<SrcInput, SrcOutput<SrcAddress>>> block, int blockHeight, String txid) throws SQLException, IOException {
        return block.getTransactions().filter((t) -> Utils.fixDupeTxid(t.getTxid(), blockHeight).equals(txid)).findAny().orElse(null);
    }

    private SrcBlock<SrcTransaction<SrcInput, SrcOutput>> preloadBlockCaches(
            SrcBlock<SrcTransaction<SrcInput, SrcOutput>> block,
            DbCachedTransaction cachedTxn,
            DbCachedOutput cachedOutput,
            DbCachedAddress cachedAddress) {
        log.trace("preloadBlockCaches({}) STARTED", block.getHeight());
        try {
            block.getTransactions()
                    .map(txn -> CompletableFuture.completedFuture(txn).thenApplyAsync(t -> preProcTransaction(t, block.getHeight(), cachedTxn, cachedOutput, cachedAddress), execTxn))
                    .collect(Collectors.toList()).forEach(CompletableFuture::join);
            return block;
        } finally {
            log.trace("preloadBlockCaches({}) FINISHED", block.getHeight());
        }
    }

    private SrcTransaction<SrcInput, SrcOutput> preProcTransaction(
            SrcTransaction<SrcInput, SrcOutput> t,
            int blockHeight,
            DbCachedTransaction cachedTxn,
            DbCachedOutput cachedOutput,
            DbCachedAddress cachedAddress) {
        long started = System.nanoTime();
        try {
            if (safeRun) {
                String txid = Utils.fixDupeTxid(t.getTxid(), blockHeight);
                getTransaction(txid, cachedTxn).ifPresent(tx -> {
                    getOutputs(tx.getTransactionId(), cachedOutput);
                    inputsCache.getUnchecked(tx.getTransactionId());
                });
            }
            List<CompletableFuture> outFutures = t.getOutputs().map(to -> CompletableFuture.runAsync(() -> getAddress(to).map(to3 -> getOrAdd(to3, cachedAddress)), execInsOuts))
                    .collect(Collectors.toList());
            if (t.getInputs() != null) {
                t.getInputs().map(ti -> CompletableFuture.runAsync(() -> getTransaction(ti.getInTxid(), cachedTxn).map(ti3 -> getOutputs(ti3.getTransactionId(), cachedOutput)), execInsOuts))
                        .collect(Collectors.toList())
                        .forEach(CompletableFuture::join);
            }
            outFutures.forEach(CompletableFuture::join);
            return t;
        } finally {
            if (System.nanoTime() - started > 800_000_000) {
                log.debug("preProcTransaction: Long running transaction. txid={}, runtime={}", t.getTxid(), Duration.ofNanos(System.nanoTime() - started));
            }
        }
    }

    @SneakyThrows(SQLException.class)
    private Optional<BtcTransaction> getTransaction(String txid, DbCachedTransaction cachedTxn) {
        return Optional.ofNullable(cachedTxn.getTransaction(txid));
    }

    @SneakyThrows(SQLException.class)
    private List<TxOutput> getOutputs(int transactionId, DbCachedOutput cachedOutput) {
        return cachedOutput.getOutputs(transactionId);
    }

    private Optional<SrcAddress> getAddress(SrcOutput to) {
        return Optional.ofNullable(to.getAddress());
    }

    @SneakyThrows(SQLException.class)
    private int getOrAdd(SrcAddress addr, DbCachedAddress cachedAddress) {
        return cachedAddress.getOrAdd(addr, true);
    }

    private boolean isTerminatingLoop() {
        if (!terminateLoop && stopFile.exists()) {
            log.info("stopFile detected: {}", stopFile.getAbsolutePath());
            terminateLoop = true;
            stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
        }
        return terminateLoop;
    }

    private static String frmtInput(TxInput in, DbCachedTransaction cachedTxn, DbCachedOutput cachedOutput, DbCachedAddress cachedAddress) throws SQLException {
        BtcTransaction tx = cachedTxn.getTransaction(in.getTransactionId());
        BtcTransaction intx = cachedTxn.getTransaction(in.getInTransactionId());
        Optional<TxOutput> out = cachedOutput.getOutput(in.getInTransactionId(), in.getInPos());
        BtcAddress adr = out.map(o -> cachedAddress.getAddress(o.getAddressId(), true)).orElse(null);
        return "Input#" + in.getPos() + " in tx " + (tx == null ? null : tx.getTxid())
                + " (" + (tx == null ? null : tx.getBlockHeight()) + "." + in.getTransactionId() + ")"
                + " pointing to output " + (intx == null ? null : intx.getTxid()) + ":" + in.getInPos()
                + " (" + (intx == null ? null : intx.getBlockHeight()) + "." + in.getInTransactionId() + ":" + in.getInPos() + ")"
                + " with address " + (adr == null ? null : adr.getBjAddress());

    }

    private static BtcTransaction findTx(Collection<BtcTransaction> list, String hash) {
        if (list == null) {
            return null;
        }
        for (BtcTransaction t : list) {
            if (t.getTxid().equalsIgnoreCase(hash)) {
                list.remove(t);
                return t;
            }
        }
        return null;
    }

    private static TxInput findInput(Collection<TxInput> list, int pos) {
        if (list == null) {
            return null;
        }
        for (TxInput t : list) {
            if (t.getPos() == pos) {
                list.remove(t);
                return t;
            }
        }
        return null;
    }

    private static TxOutput findOutput(Collection<TxOutput> list, int pos) {
        if (list == null) {
            return null;
        }
        for (TxOutput t : list) {
            if (t.getPos() == pos) {
                list.remove(t);
                return t;
            }
        }
        return null;
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
