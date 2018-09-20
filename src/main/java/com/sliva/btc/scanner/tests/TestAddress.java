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

import com.sliva.btc.scanner.util.BJBlockHandler;
import org.apache.commons.codec.binary.Hex;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.LegacyAddress;

/**
 *
 * @author Sliva Co
 */
public class TestAddress {

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        Address a = LegacyAddress.fromPubKeyHash(BJBlockHandler.getNetworkParams(), Hex.decodeHex("dfba28dc856c9a949fb67a55b234adb957416dfe"));
//        a = LegacyAddress.fromScriptHash(BJBlockHandler.getNetworkParams(), Hex.decodeHex("a4da1cd6f83fb5b1d0d7d6b5aba348eef8973faf"));
        System.out.println("address=" + a);
    }

}
