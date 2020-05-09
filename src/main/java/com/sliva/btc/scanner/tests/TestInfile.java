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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import java.lang.reflect.Field;
import java.util.Arrays;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Base58;
import org.bitcoinj.core.Bech32;
import org.bitcoinj.core.Bech32.Bech32Data;
import org.bitcoinj.core.LegacyAddress;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.SegwitAddress;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.spongycastle.util.encoders.Hex;

/**
 *
 * @author Sliva Co
 */
public class TestInfile {

    private static final DBConnectionSupplier dbConOld = new DBConnectionSupplier("btc");
    private static final ThreadLocal<NetworkParameters> np = new ThreadLocal<NetworkParameters>() {
        @Override
        protected NetworkParameters initialValue() {
            return new MainNetParams();
        }
    };

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        //dbConOld.get().prepareCall("LOAD DATA LOCAL INFILE '/ProgramData/MySQL/MySQL Server 8.0/Uploads/addr.txt' INTO TABLE address_test").execute();
        bc1addr("bc1qjmyx8fpqc0j3nta7p24st2aa84mnd998m0pc3vsg6454t6j5funskg0hfk");
        bc1addr("bc1qpkuy609cpcl7dpvrgkpavgtdqumtcynxp33z0h");

        legAddr("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa");
        legAddr("31h1vvHoD44tcKQJYwMm95BbLvux7qePEm");
    }

    private static void legAddr(String src) throws Exception {
        System.out.println("\n\nsrc=" + src);
        Address a = Address.fromString(np.get(), src);
        System.out.println("  a=" + a);
        System.out.println("a.hash.len=" + a.getHash().length);
        System.out.println("a.type=" + a.getOutputScriptType());
        LegacyAddress la = a.getOutputScriptType() == Script.ScriptType.P2SH
                ? LegacyAddress.fromScriptHash(np.get(), a.getHash())
                : LegacyAddress.fromPubKeyHash(np.get(), a.getHash());
        System.out.println("la.type=" + la.getOutputScriptType());
        System.out.println(" la=" + la);
        System.out.println("la.type=" + la.getOutputScriptType());

        byte[] b = Base58.decodeChecked(src);
        System.out.println("Version byte: " + b[0]);
        byte[] data = Arrays.copyOfRange(b, 1, b.length);
        System.out.println("Hex: " + Hex.toHexString(data));
        System.out.println("len=" + data.length);
        String s = Base58.encodeChecked(b[0], data);
        System.out.println("s=" + s);
    }

    private static void bc1addr(String src) throws Exception {
        System.out.println("\n\nsrc=" + src);
        Address a = Address.fromString(np.get(), src);
        System.out.println("  a=" + a);
        System.out.println("a.hash.len=" + a.getHash().length);
        System.out.println("a.type=" + a.getOutputScriptType());

        SegwitAddress swa = SegwitAddress.fromHash(np.get(), a.getHash());
        System.out.println("swa=" + swa);
        System.out.println("swa.type=" + swa.getOutputScriptType());

        Bech32Data be = Bech32.decode(src);
        Field f = Bech32Data.class.getDeclaredField("data");
        f.setAccessible(true);
        byte[] bedata = (byte[]) f.get(be);
        System.out.println("be=" + Hex.toHexString(bedata));
        System.out.println("len=" + bedata.length);
        System.out.println("src=" + src + ", size=" + src.length());
    }
}
