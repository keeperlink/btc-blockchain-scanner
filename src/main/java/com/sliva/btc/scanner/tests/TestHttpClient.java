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
package com.sliva.btc.scanner.tests;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.bitcoinj.core.Utils;

/**
 *
 * @author Sliva Co
 */
public class TestHttpClient {

    private static final String RPCUSER = "cer6Yxq2apNc";
    private static final String RPCPASSWORD = "hY7cZp01WmCbvqPiyaLY67x";
    private static final String AUTH = RPCUSER + ":" + RPCPASSWORD;
    private static final String BTC_ADDRESS = "http://@localhost:17955";

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        HttpClient httpclient = new HttpClient();
        URL u = new URL(BTC_ADDRESS);
        System.out.println("u=" + u);
        String auth = Base64.getEncoder().encodeToString(AUTH.getBytes(StandardCharsets.ISO_8859_1));

        for (int i = 0; i < 100000; i++) {
            PostMethod httppost = new PostMethod(BTC_ADDRESS);
            httppost.addRequestHeader("Authorization", "Basic " + auth);
            StringRequestEntity params = new StringRequestEntity("{\"method\":\"getblockchaininfo\",\"params\":[],\"id\":\"1\"}", "application/json", "utf-8");
            httppost.setRequestEntity(params);

            try {
                int respCode = httpclient.executeMethod(httppost);
//                System.out.println("respCode: " + respCode);
                String response = httppost.getResponseBodyAsString();
//                System.out.println("Response Headers: " + Arrays.deepToString(httppost.getResponseHeaders()));
//                System.out.println("response: " + response);
                // consume the response entity
            } finally {
                httppost.releaseConnection();
                Utils.sleep(5);
            }
        }
    }

}
