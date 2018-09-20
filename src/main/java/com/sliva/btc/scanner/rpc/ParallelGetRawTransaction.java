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
package com.sliva.btc.scanner.rpc;

import com.sliva.btc.scanner.db.model.BtcTransaction;
import com.sliva.btc.scanner.util.Utils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;
import wf.bitcoin.javabitcoindrpcclient.GenericRpcException;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class ParallelGetRawTransaction {

    private static final int MAX_QUEUE_SIZE = 30000;
    private static final long STORE_MAX_WAIT_TIME_MSEC = 30 * 1000L;
    private final int nThreads;
    private final Iterator<BtcTransaction> txIterator;
    private final Map<String, RawTransaction> txQueue = new HashMap<>();

    public ParallelGetRawTransaction(int nThreads, Collection<BtcTransaction> transactions) {
        this.nThreads = nThreads;
        txIterator = transactions.iterator();
        startThreads();
    }

    public RawTransaction getRawTransaction(String txid) {
        for (;;) {
            synchronized (txQueue) {
                RawTransaction rt = txQueue.get(txid);
                if (rt != null) {
//                    log.info("getBlock: height=" + block.height() + ", blockQueue.size=" + blockQueue.size());
                    txQueue.remove(txid);
                    return rt;
                }
            }
            sleepHere(1);
        }
    }

    public int getQueueSize() {
        return txQueue.size();
    }

    private void startThreads() {
        for (int i = 0; i < nThreads; i++) {
            new ParallelGetRawTxThread(i).start();
        }
    }

    private void storeNewTransaction(RawTransaction rawTransaction) {
//        log.info("\t " + Thread.currentThread().getName() + "\t storeNewTransaction: txid=" + rawTransaction.txId() + ",\t blockQueue.size=" + txQueue.size());
        long timeout = System.currentTimeMillis() + STORE_MAX_WAIT_TIME_MSEC;
        while (txQueue.size() >= MAX_QUEUE_SIZE && System.currentTimeMillis() < timeout) {
            sleepHere(100);
        }
        synchronized (txQueue) {
            txQueue.put(rawTransaction.txId(), rawTransaction);
        }
    }

    private static void sleepHere(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private class ParallelGetRawTxThread extends Thread {

        private final RpcClient client = new BtcClientDiskCachedBlock();
        private final int threadNum;

        public ParallelGetRawTxThread(int threadNum) {
            super("ParallelGetRawTxThread#" + threadNum);
            this.threadNum = threadNum;
        }

        @Override
        @SuppressWarnings("CallToPrintStackTrace")
        public void run() {
            log.info("Thread " + getName() + " STARTED.");
            for (;;) {
                while (txQueue.size() > (nThreads - threadNum) * MAX_QUEUE_SIZE / nThreads) {
                    sleepHere(10);
                }
                BtcTransaction bt;
                synchronized (txIterator) {
                    if (!txIterator.hasNext()) {
                        break;
                    }
                    bt = txIterator.next();
                }
                RawTransaction tx = null;
                for (int i = 0; i < 20; i++) {
                    try {
                        tx = client.getRawTransaction(Utils.unfixDupeTxid(bt.getTxid()));
                    } catch (GenericRpcException e) {
                        e.printStackTrace();
                        sleepHere(5000);
                    }
                }
                if (tx == null) {
                    throw new IllegalStateException("Block retrieval failed: " + bt.getTxid());
                }
                storeNewTransaction(tx);
            }
            log.info("Thread " + getName() + " FINISHED.");
        }
    }
}
