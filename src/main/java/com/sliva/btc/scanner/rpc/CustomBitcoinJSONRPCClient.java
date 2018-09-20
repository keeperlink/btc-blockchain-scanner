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

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import wf.bitcoin.javabitcoindrpcclient.BitcoinJSONRPCClient;
import wf.bitcoin.javabitcoindrpcclient.BitcoinRPCException;
import wf.bitcoin.javabitcoindrpcclient.GenericRpcException;
import wf.bitcoin.krotjson.JSON;

/**
 *
 * @author Sliva Co
 */
public class CustomBitcoinJSONRPCClient extends BitcoinJSONRPCClient {

    private static final ThreadLocal<HttpClient> clientPool = ThreadLocal.withInitial(() -> new HttpClient());
    private final String url;
    private final String auth;

    public CustomBitcoinJSONRPCClient(String url, String user, String password) {
        this.url = url;
        this.auth = Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.ISO_8859_1));
    }

    @Override
    public Object query(String method, Object... o) throws GenericRpcException {
        final String reqId = "1";
        String req = JSON.stringify(new LinkedHashMap() {
            {
                put("method", method);
                put("params", o);
                put("id", reqId);
            }
        });
        try {
            PostMethod httpPost = new PostMethod(url);
            httpPost.addRequestHeader("Authorization", "Basic " + auth);
            StringRequestEntity params = new StringRequestEntity(req, "application/json", "utf-8");
            httpPost.setRequestEntity(params);
            int respCode = clientPool.get().executeMethod(httpPost);
            if (respCode != 200) {
                throw new BitcoinRPCException(method, Arrays.deepToString(o), respCode, "", httpPost.getResponseBodyAsString());
            }
            Map response = (Map) JSON.parse(httpPost.getResponseBodyAsString());

            if (!reqId.equals(response.get("id"))) {
                throw new BitcoinRPCException("Wrong response ID (expected: " + String.valueOf(reqId) + ", response: " + response.get("id") + ")");
            }

            if (response.get("error") != null) {
                throw new GenericRpcException(JSON.stringify(response.get("error")));
            }
            return response.get("result");
        } catch (IOException ex) {
            throw new BitcoinRPCException(method, Arrays.deepToString(o), ex);
        }
    }

}
