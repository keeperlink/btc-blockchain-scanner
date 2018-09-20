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

import java.util.LinkedHashMap;
import java.util.Map;
import wf.bitcoin.javabitcoindrpcclient.BitcoindRpcClient.RawTransaction;
import wf.bitcoin.javabitcoindrpcclient.GenericRpcException;

/**
 *
 * @author Sliva Co
 */
public class BtcClientMemCachedTransaction extends RpcClient {

    public static int MAX_CACHE_SIZE = 20000;
    private final Map<String, RawTransaction> mapCache = new LinkedHashMap<>();

    public BtcClientMemCachedTransaction() {
    }

    @Override
    public RawTransaction getRawTransaction(String txId) throws GenericRpcException {
        RawTransaction result = readFromCache(txId);
        if (result == null) {
            result = super.getRawTransaction(txId);
            saveToCache(result, txId);
        }
        return result;
    }

    private RawTransaction readFromCache(String txId) {
        synchronized (mapCache) {
            return mapCache.get(txId);
        }
    }

    private void saveToCache(RawTransaction t, String txId) {
        synchronized (mapCache) {
            if (!mapCache.containsKey(txId)) {
                mapCache.put(txId, t);
                if (mapCache.size() > MAX_CACHE_SIZE) {
                    mapCache.remove(mapCache.keySet().iterator().next());
                }
            }
        }
    }
}
