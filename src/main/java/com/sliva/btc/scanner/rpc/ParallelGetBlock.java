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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Block;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class ParallelGetBlock {

    private static final int MAX_QUEUE_SIZE = 2990;
    private static final long STORE_MAX_WAIT_TIME_MSEC = 60 * 1000L;
    private final int nThreads;
//    private final int firstBlock;
//    private final int numBlocks;
    private final int lastBlock;
    private final int delta;
    private final AtomicInteger currentBlock = new AtomicInteger();
    private final Map<Integer, Block> blockQueue = new HashMap<>();

    public ParallelGetBlock(int nThreads, int firstBlock, int numBlocks) {
        this.nThreads = nThreads;
//        this.firstBlock = firstBlock;
//        this.numBlocks = numBlocks;
        this.delta = numBlocks > 0 ? 1 : -1;
        this.lastBlock = firstBlock + numBlocks;
        this.currentBlock.set(firstBlock);
        startThreads();
    }

    public Block getBlock(int height) {
        for (;;) {
            synchronized (blockQueue) {
                Block block = blockQueue.get(height);
                if (block != null) {
//                    log.info("getBlock: height=" + block.height() + ", blockQueue.size=" + blockQueue.size());
                    blockQueue.remove(block.height());
                    return block;
                }
            }
            sleepHere(1);
        }
    }

    public int getQueueSize() {
        return blockQueue.size();
    }

    private void startThreads() {
        for (int i = 0; i < nThreads; i++) {
            new ParallelGetBlockThread(i).start();
        }
    }

    private void storeNewBlock(BitcoindRpcClient.Block block) {
        log.info("\t " + Thread.currentThread().getName() + "\t ParallelGetBlock.storeNewBlock: height=" + block.height() + ",\t blockQueue.size=" + blockQueue.size());
        long timeout = System.currentTimeMillis() + STORE_MAX_WAIT_TIME_MSEC;
        while (blockQueue.size() >= MAX_QUEUE_SIZE && System.currentTimeMillis() < timeout) {
            sleepHere(100);
        }
        synchronized (blockQueue) {
            blockQueue.put(block.height(), block);
        }
    }

    private static void sleepHere(int msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private class ParallelGetBlockThread extends Thread {

        private final RpcClient client = new BtcClientDiskCachedBlock();
        private final int threadNum;

        public ParallelGetBlockThread(int threadNum) {
            super("ParallelGetBlockThread#" + threadNum);
            this.threadNum = threadNum;
        }

        @Override
        @SuppressWarnings("CallToPrintStackTrace")
        public void run() {
            log.info("Thread " + getName() + " STARTED.");
            for (;;) {
                while (blockQueue.size() > (nThreads - threadNum) * MAX_QUEUE_SIZE / nThreads) {
                    sleepHere(10);
                }
                int blockHeight;
                synchronized (currentBlock) {
                    if (currentBlock.get() == lastBlock) {
                        break;
                    }
                    blockHeight = currentBlock.getAndAdd(delta);
                }
                Block block = null;
                for (int i = 0; i < 20; i++) {
                    try {
                        block = client.getBlock(blockHeight);
                    } catch (Exception e) {
                        e.printStackTrace();
                        sleepHere(5000);
                    }
                }
                if (block == null) {
                    throw new IllegalStateException("Block retrieval failed: " + blockHeight);
                }
                storeNewBlock(block);
            }
            log.info("Thread " + getName() + " FINISHED.");
        }

    }
}
