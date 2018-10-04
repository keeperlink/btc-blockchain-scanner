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
import com.sliva.btc.scanner.util.Utils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author whost
 */
@Slf4j
public class RpcClientDirect {

    public static String RPC_URL = "http://localhost:17955";
    public static String RPC_USER = "user";
    public static String RPC_PASSWORD = "password";
    private static RpcClientDirect instance;
    private static final ThreadLocal<HttpClient> clientPool = ThreadLocal.withInitial(() -> new HttpClient());
    private final AtomicLong reqCounter = new AtomicLong();
    private final String auth;

    public static RpcClientDirect getInstance() {
        if (instance == null) {
            instance = new RpcClientDirect();
        }
        return instance;
    }

    public RpcClientDirect() {
        this.auth = Base64.getEncoder().encodeToString((RPC_USER + ":" + RPC_PASSWORD).getBytes(StandardCharsets.ISO_8859_1));
        log.trace("new RpcClientDirect(): user={}, password=*****", RPC_USER);
    }

    public String getBlockHash(int height) throws IOException {
        return query("getblockhash", height).toString();
    }

    public int getBlockHeight(String hash) throws IOException {
        return Double.valueOf(((Map) query("getblock", hash, 1)).get("height").toString()).intValue();
    }

    public String getRawBlock(String hash) throws IOException {
        return query("getblock", hash, 0).toString();
    }

    public Object query(String method, Object... params) throws IOException {
        final String reqId = Long.toString(reqCounter.incrementAndGet());
        String req = new Gson().toJson(new RpcRequest(method, params, reqId));
        log.trace("query(method:{}): Request: {}", method, req);
        PostMethod httpPost = new PostMethod(RPC_URL);
        httpPost.addRequestHeader("Authorization", "Basic " + auth);
        httpPost.setRequestEntity(new StringRequestEntity(req, "application/json", "utf-8"));
        int respCode = clientPool.get().executeMethod(httpPost);
        if (respCode != 200) {
            throw new IOException("Response code not OK: " + respCode + ". method=" + method + ", params=" + Arrays.deepToString(params) + ", response: " + httpPost.getResponseBodyAsString());
        }
        Map response = new Gson().fromJson(IOUtils.toString(httpPost.getResponseBodyAsStream(), StandardCharsets.UTF_8), Map.class);
        log.trace("query(method:{}): Request: {}", method, response);
        if (!reqId.equals(response.get("id"))) {
            throw new IOException("Wrong response ID (expected: " + String.valueOf(reqId) + ", response: " + response.get("id") + ")");
        }
        if (response.get("error") != null) {
            throw new IOException(new Gson().toJson(response.get("error")));
        }
        return response.get("result");
    }

    public static void applyArguments(CommandLine cmd) {
        Properties prop = Utils.loadProperties(cmd.getOptionValue("rpc-config"));
        RPC_URL = cmd.getOptionValue("rpc-url", prop.getProperty("rpc-url", RPC_URL));
        RPC_USER = cmd.getOptionValue("rpc-user", prop.getProperty("rpc-user", RPC_USER));
        RPC_PASSWORD = cmd.getOptionValue("rpc-password", prop.getProperty("rpc-password", RPC_PASSWORD));
        log.trace("applyArguments(): url={}, user={}, password=*****", RPC_URL, RPC_USER);
    }

    public static Options addOptions(Options options) {
        options.addOption("r", "rpc-url", true, "RPC URL to running bitcoin core. Default is '" + RpcClient.RPC_URL + "'.");
        options.addOption("x", "rpc-user", true, "RPC user name.");
        options.addOption("y", "rpc-password", true, "RPC password.");
        options.addOption(null, "rpc-config", true, "Configuration file name with RPC url, user and password values.");
        return options;
    }

    @Getter
    @AllArgsConstructor
    private static class RpcRequest {

        private final String method;
        private final Object[] params;
        private final String id;
    }

    @Data
    private static class RpcResponseBlock {

        private final String hash;
        private final int height;
    }
}
