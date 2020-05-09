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

import com.sliva.btc.scanner.util.BJBlockHandler;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.bitcoinj.core.Block;
import org.bitcoinj.utils.BlockFileLoader;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunPrepareBlockFiles {

    private static final String BITCOIN_CORE_BLOCKS_DIR = "/CryptoData/Bitcoin/blocks";
    private static final String RAW_BLOCKS_DESTINATION_DIR = "/CryptoData/btc-scanner/full_blocks";
    private static final int THREADS = 12;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    @SuppressWarnings({"null", "CallToPrintStackTrace"})
    public static void main(String[] args) throws Exception {
        File readDir = new File(BITCOIN_CORE_BLOCKS_DIR);
        File saveDir = new File(RAW_BLOCKS_DESTINATION_DIR);
        log.debug("START");
//            Block block2 = new BitcoinSerializer(np, false).makeBlock(block.unsafeBitcoinSerialize());
//            block = block2;

//        File[] files = readDir.listFiles((f) -> f.getName().startsWith("blk")
//        //                && f.getName().compareTo("blk00000.dat") >= 0
//        );
        Collection<File> files = BlockFileLoader.getReferenceClientBlockFileList(readDir);
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        files.stream().forEach(ff -> {
            log.info("File: " + ff);
            executor.execute(() -> {
                log.info("Processing File: " + ff);
                List<File> blockChainFiles = new ArrayList<>();
                blockChainFiles.add(ff);
                BlockFileLoader bfl = new BlockFileLoader(BJBlockHandler.getNetworkParams(), blockChainFiles);
                for (Block block : bfl) {
                    String blockHash = block.getHashAsString();
                    File f = new File(saveDir, blockHash + ".block");
                    if (f.exists()) {
                        continue;
                    }
                    try (OutputStream out = new FileOutputStream(f)) {
                        out.write(block.unsafeBitcoinSerialize());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        });
        executor.shutdown();
        log.info("Waiting completion...");
        executor.awaitTermination(999, TimeUnit.DAYS);
        log.info("Done");
    }
}
