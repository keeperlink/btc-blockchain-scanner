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

import com.google.gson.Gson;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.text.DecimalFormat;
import lombok.extern.slf4j.Slf4j;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.Block;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class BtcClientDiskCachedBlock extends RpcClient {

    private static final String CACHE_PATH = "/CryptoData/btc-scanner/getBlock.cache";

    @Override
    public Block getBlock(int height) {
        Block block = readFromCache(height);
        if (block == null) {
            block = super.getBlock(height);
            if (block != null) {
                saveToCache(block);
            }
        }
        return block;
    }

    private static Block readFromCache(int height) {
        File f = new File(CACHE_PATH, new DecimalFormat("0000000").format(height) + ".json");
        if (f.exists()) {
            try (Reader r = new FileReader(f)) {
                return new Gson().fromJson(r, BlockMapWrapper.class);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        return null;
    }

    private static void saveToCache(Block block) {
        File f = new File(CACHE_PATH, new DecimalFormat("0000000").format(block.height()) + ".json");
        String jsonStr = new Gson().toJson(block);
        try (Writer w = new FileWriter(f)) {
            w.write(jsonStr);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
