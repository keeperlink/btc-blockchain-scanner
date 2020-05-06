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
import com.sliva.btc.scanner.db.DBConnection;
import com.sliva.btc.scanner.db.DbAddBlock;
import com.sliva.btc.scanner.db.DbCachedAddress;
import com.sliva.btc.scanner.db.DbCachedOutput;
import com.sliva.btc.scanner.db.DbCachedTransaction;
import com.sliva.btc.scanner.db.DbQueryBlock;
import com.sliva.btc.scanner.db.DbQueryInput;
import com.sliva.btc.scanner.db.DbQueryInputSpecial;
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
import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdOption;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.Utils;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
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

    public static final Collection<CmdOption> CMD_OPTS = new ArrayList<>();
    private static final CmdOption safeRunOpt = buildOption(CMD_OPTS, null, "safe-run", true, "Run in safe mode - check DB for existing records before adding new");
    private static final CmdOption updateSpentOpt = buildOption(CMD_OPTS, null, "update-spent", true, "Update spent flag on outpus. Default is true. For better performance of massive update you might want to disable it and run separate process after this update is done.");
    private static final CmdOption blocksBackOpt = buildOption(CMD_OPTS, null, "blocks-back", true, "Check last number of blocks. Process will run in safe mode (option --safe-run=true)");
    private static final CmdOption startFromBlockOpt = buildOption(CMD_OPTS, null, "start-from-block", true, "Start checking from block hight provided. Process will run in safe mode (option --safe-run=true)");
    private static final CmdOption runToBlockOpt = buildOption(CMD_OPTS, null, "run-to-block", true, "Last block number to run");
    private static final CmdOption threadsOpt = buildOption(CMD_OPTS, null, "threads", true, "Number of threads to run. Default is " + DEFAULT_TXN_THREADS + ". To disable parallel threading set value to 0");
    private static final CmdOption stopFileOpt = buildOption(CMD_OPTS, null, "stop-file", true, "File to be watched on each new block to stop process. If file is present the process stops and file renamed by adding '1' to the end.");
    private static final CmdOption loopOpt = buildOption(CMD_OPTS, null, "loop", true, "Repeat update every provided number of seconds");

    static {
        CMD_OPTS.addAll(DBConnection.CMD_OPTS);
        CMD_OPTS.addAll(RpcClient.CMD_OPTS);
        CMD_OPTS.addAll(BJBlockProvider.CMD_OPTS);
    }

    private final File stopFile;
    private final boolean safeRun;
    private final boolean runParallel;
    private final boolean updateSpent;
    private final ExecutorService futureExecutor;
    private final ExecutorService execTxn;
    private final DBConnection dbCon;
    private final DbQueryBlock queryBlock;
    private final DbQueryInput queryInput;
    private final DbQueryInputSpecial queryInputSpecial;
    private final BlockProvider blockProvider;
    private final Optional<Integer> startBlock;
    private final Optional<Integer> lastBlock;
    private final int blocksBack;
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
            Integer loopTime = cmd.getOption(loopOpt).map(Integer::parseInt).orElse(null);
            do {
                new RunFullScan(cmd).runProcess();
                if (loopTime != null && loopTime > 0 && !terminateLoop) {
                    log.info("Execution finished. Sleeping for " + loopTime + " sec...");
                    Thread.sleep(loopTime * 1000L);
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
        futureExecutor = Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("BlockReader-%d").build());
        int nExecTxnThreads = cmd.getOption(threadsOpt).map(Integer::parseInt).orElse(DEFAULT_TXN_THREADS);
        runParallel = nExecTxnThreads != 0;
        execTxn = runParallel ? Executors.newFixedThreadPool(nExecTxnThreads,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecTxn-%d").build()) : null;
        DBConnection.applyArguments(cmd);
        BJBlockProvider.applyArguments(cmd);
        RpcClient.applyArguments(cmd);
        RpcClientDirect.applyArguments(cmd);
        dbCon = new DBConnection();
        queryBlock = new DbQueryBlock(dbCon);
        queryInput = new DbQueryInput(dbCon);
        queryInputSpecial = new DbQueryInputSpecial(dbCon);
        if (cmd.hasOption(BJBlockProvider.fullBlocksPathOpt)) {
            blockProvider = new BlockProviderWithBackup(new BJBlockProvider(), new RpcBlockProvider());
        } else {
            blockProvider = new RpcBlockProvider();
        }
        inputsCache = !safeRun ? null : CacheBuilder.newBuilder()
                .concurrencyLevel(Math.max(1, nExecTxnThreads))
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
            Future<FutureBlock> futureBlock = null, futureBlock2 = null;
            if (runParallel) {
                futureBlock = getFutureBlock(firstBlockToProcess, null, cachedTxn, cachedOutput, cachedAddress);
                futureBlock2 = getFutureBlock(firstBlockToProcess + 1, futureBlock, cachedTxn, cachedOutput, cachedAddress);
            }
            for (int blockHeight = firstBlockToProcess; blockHeight <= lastBlockToProcess; blockHeight++) {
                if (stopFile.exists()) {
                    terminateLoop = true;
                    log.info("Exiting - stop file found: " + stopFile.getAbsolutePath());
                    if (futureBlock != null) {
                        futureBlock.get();
                        if (futureBlock2 != null) {
                            futureBlock2.get();
                        }
                        futureExecutor.shutdown();
                        if (execTxn != null) {
                            execTxn.shutdown();
                        }
                    }
                    stopFile.renameTo(new File(stopFile.getAbsoluteFile() + "1"));
                    break;
                }
                SrcBlock<SrcTransaction<SrcInput, SrcOutput<SrcAddress>>> block;
                if (futureBlock != null) {
                    FutureBlock fb = futureBlock.get();
                    if (fb.getBlockHeight() != blockHeight) {
                        throw new IllegalArgumentException("Feature block height doesn't match: " + fb.getBlockHeight() + " != " + blockHeight);
                    }
                    block = fb.getBlock();
                } else {
                    block = blockProvider.getBlock(blockHeight);
                }

                if (runParallel) {
                    futureBlock = futureBlock2;
                    futureBlock2 = getFutureBlock(blockHeight + 2, futureBlock, cachedTxn, cachedOutput, cachedAddress);
                }

                String blockHash = block.getHash();
                log.info("Block(" + blockHeight + ").hash: " + blockHash + ", nTxns=" + block.getTransactions().count());
                if (queryBlock.findBlockByHash(blockHash) == null) {
                    addBlock.add(BtcBlock.builder()
                            .height(blockHeight)
                            .hash(blockHash)
                            .txnCount((int) block.getTransactions().count())
                            .build());
                }
                List<BtcTransaction> listTxn = safeRun ? cachedTxn.getTransactionsInBlock(blockHeight) : null;
                for (SrcTransaction t : block.getTransactions().collect(Collectors.toList())) {
                    processTransaction(t, blockHeight, listTxn, updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput);
                }
                if (listTxn != null && !listTxn.isEmpty()) {
                    log.debug("Found wrong transactions in block: " + listTxn);
                    for (BtcTransaction t : listTxn) {
                        cachedTxn.delete(t);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            futureExecutor.shutdownNow();
            if (execTxn != null) {
                execTxn.shutdownNow();
            }
            log.info("Execution FINISHED");
        }
    }

    @SneakyThrows(SQLException.class)
    private int getFirstUnprocessedBlockFromDB() {
        return queryBlock.findLastHeight() + 1;
    }

    @Getter
    @Builder
    @ToString
    private static class TxnProcessOutput {

        private final BtcTransaction tx;
        private final Collection<TxInput> badInputs;
        private final Collection<TxOutput> badOutputs;
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

    private TxnProcessOutput processTransaction(
            SrcTransaction t,
            int blockHeight,
            List<BtcTransaction> listTxn,
            DbUpdateInput updateInput,
            DbUpdateInputSpecial updateInputSpecial,
            DbCachedTransaction cachedTxn,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) throws SQLException, ExecutionException {
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
        }//TODO validate
        return TxnProcessOutput.builder()
                .tx(btcTx)
                .badInputs(processTransactionInputs(t, btcTx, updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput))
                .badOutputs(processTransactionOutputs(t, btcTx, cachedTxn, cachedAddress, cachedOutput))
                .build();
    }

    @SuppressWarnings("UseSpecificCatch")
    private Collection<TxInput> processTransactionInputs(
            SrcTransaction<SrcInput, SrcOutput<SrcAddress>> t,
            BtcTransaction tx,
            DbUpdateInput updateInput,
            DbUpdateInputSpecial updateInputSpecial,
            DbCachedTransaction cachedTxn,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) throws SQLException, ExecutionException {
        if (t.getInputs() == null) {
            return null;
        }
//        log.trace("Tx.inputs: {}", t.getInputs().size());
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
                TxOutput txOutput = cachedOutput.getOutput(inTxn.getTransactionId(), inPos);
                if (txOutput == null) {
                    throw new IllegalStateException("Output#" + ti.getPos() + " not found: " + inTxid + ":" + inPos + ". Src txn: " + tx.getTxid() + ". Ref tx: " + inTxn);
                }
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
                                TxnProcessOutput out = processTransaction(in2.getTransactionId(), updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput);
                                //updateInput.delete(in2);
//                        if (out.badInputs != null && out.badInputs.contains(in2)) {
//                            //it was a bad input - proceed
//                        } else {
//                            throw new IllegalStateException("Error adding input " + inputToAdd + ". DB consistency issue: Found in DB input with same connected output: " + in2);
//                        }
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
                                updateInput.executeUpdate();
//                            txInput = inputToAdd;
                            } else {
                                try {
                                    TxnProcessOutput out = processTransaction(txInput.getInTransactionId(), updateInput, updateInputSpecial, cachedTxn, cachedAddress, cachedOutput);
                                    log.info("out={}", out);
                                } catch (Exception e) {
                                    log.debug(e.getMessage(), e);
                                }
                                updateInput.update(inputToAdd);
                                updateInput.executeUpdate();
//                            txInput = inputToAdd;
                                //throw new IllegalStateException("Error validating input \n" + inputToAdd + ". In DB found another input record with different data: \n" + txInput);
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
     * @param cachedTxn DbCachedTransaction
     * @param cachedAddress DbCachedAddress
     * @param cachedOutput DbCachedOutput
     * @return List of transactions from DB that not part of Transaction
     * @throws SQLException
     */
    private Collection<TxOutput> processTransactionOutputs(
            SrcTransaction<SrcInput, SrcOutput<SrcAddress>> t,
            BtcTransaction tx,
            DbCachedTransaction cachedTxn,
            DbCachedAddress cachedAddress,
            DbCachedOutput cachedOutput) throws SQLException {
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

    /**
     * Pre-load data for the next loop.
     *
     * @param blockHeight
     * @param qBlock
     * @param cachedTxn
     * @param cachedOutput
     * @param cachedAddress
     * @return
     */
    @SuppressWarnings({"UseSpecificCatch", "null"})
    private Future<FutureBlock> getFutureBlock(
            int blockHeight,
            Future<FutureBlock> prevFutureBlock,
            DbCachedTransaction cachedTxn,
            DbCachedOutput cachedOutput,
            DbCachedAddress cachedAddress) {
        return futureExecutor.submit(() -> {
            SrcBlock<SrcTransaction<SrcInput, SrcOutput<SrcAddress>>> block = blockProvider.getBlock(blockHeight);
//            if (prevFutureBlock != null) {
//                prevFutureBlock.get();
//            }
            execTxn.invokeAll(block.getTransactions().map(t -> new PreProcTransaction(t, blockHeight, cachedTxn, cachedOutput, cachedAddress)).collect(Collectors.toList()));
            return FutureBlock.builder()
                    .blockHeight(blockHeight)
                    .block(block)
                    .build();
        });
    }

    @AllArgsConstructor
    private class PreProcTransaction implements Callable<Boolean> {

        private final SrcTransaction<SrcInput, SrcOutput<SrcAddress>> t;
        private final int blockHeight;
        private final DbCachedTransaction cachedTxn;
        private final DbCachedOutput cachedOutput;
        private final DbCachedAddress cachedAddress;

        @Override
        @SuppressWarnings("UseSpecificCatch")
        public Boolean call() throws Exception {
            if (safeRun) {
                String txid = Utils.fixDupeTxid(t.getTxid(), blockHeight);
                BtcTransaction tx = cachedTxn.getTransaction(txid);
                if (tx != null) {
                    cachedOutput.getOutputs(tx.getTransactionId());
                    inputsCache.get(tx.getTransactionId());
                }
            }
            if (t.getInputs() != null) {
                t.getInputs().forEach((ti) -> {
                    try {
                        BtcTransaction inTxn = cachedTxn.getTransaction(ti.getInTxid());
                        if (inTxn != null) {
                            cachedOutput.getOutputs(inTxn.getTransactionId());
                        }
                    } catch (Exception e) {
                        //ignore
                    }
                });
            }
            t.getOutputs().forEach((to) -> {
                try {
                    String txid = Utils.fixDupeTxid(t.getTxid(), blockHeight);
                    SrcAddress addrStr = to.getAddress();
                    cachedAddress.getOrAdd(addrStr, true);
                } catch (Exception e) {
                }
            });
            return true;
        }
    }

    private String frmtInput(TxInput in, DbCachedTransaction cachedTxn, DbCachedOutput cachedOutput, DbCachedAddress cachedAddress) throws SQLException {
        BtcTransaction tx = cachedTxn.getTransaction(in.getTransactionId());
        BtcTransaction intx = cachedTxn.getTransaction(in.getInTransactionId());
        TxOutput out = cachedOutput.getOutput(in.getInTransactionId(), in.getInPos());
        BtcAddress adr = out == null ? null : cachedAddress.getAddress(out.getAddressId(), true);
        return "Input#" + in.getPos() + " in tx " + (tx == null ? null : tx.getTxid())
                + " (" + (tx == null ? null : tx.getBlockHeight()) + "." + in.getTransactionId() + ")"
                + " pointing to output " + (intx == null ? null : intx.getTxid()) + ":" + in.getInPos()
                + " (" + (intx == null ? null : intx.getBlockHeight()) + "." + in.getInTransactionId() + ":" + in.getInPos() + ")"
                + " with address " + (adr == null ? null : adr.getBjAddress());

    }

    private BtcTransaction findTx(Collection<BtcTransaction> list, String hash) {
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

    private TxInput findInput(Collection<TxInput> list, int pos) {
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

    private TxOutput findOutput(Collection<TxOutput> list, int pos) {
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
    private static class FutureBlock {

        private final int blockHeight;
        private final SrcBlock block;
    }
}
