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
package com.sliva.btc.scanner.util;

import com.sliva.btc.scanner.src.SrcAddressType;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bitcoinj.script.Script;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public final class Utils {

    private static final String DUPE1 = "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468";
    private static final String DUPE1_REPLACE = "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb467";
    private static final int DUPE1_BLOCK = 91722;
    private static final String DUPE2 = "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599";
    private static final String DUPE2_REPLACE = "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88598";
    private static final int DUPE2_BLOCK = 91812;

    public static String fixDupeTxid(String txid, int blockHeight) {
        if (blockHeight == DUPE1_BLOCK && txid.equalsIgnoreCase(DUPE1)) {
            return DUPE1_REPLACE;
        } else if (blockHeight == DUPE2_BLOCK && txid.equalsIgnoreCase(DUPE2)) {
            return DUPE2_REPLACE;
        }
        return txid;
    }

    public static String unfixDupeTxid(String txid) {
        return DUPE1_REPLACE.equalsIgnoreCase(txid)
                ? DUPE1
                : DUPE2_REPLACE.equalsIgnoreCase(txid)
                ? DUPE2
                : txid;
    }

    public static String fixAddr(String a) {
        return a;
    }

    public static String unfixAddr(String a) {
        return a;
    }

    public static void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static String id2hex(byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length != 32) {
            throw new IllegalArgumentException("Txid or blockid has to be 32 bytes long: " + Hex.encodeHexString(data));
        }
        return Hex.encodeHexString(data);
    }

    public static byte[] id2bin(String txid) {
        if (txid == null) {
            return null;
        }
        if (txid.length() != 64) {
            throw new IllegalArgumentException("Txid or blockid has to be 32 bytes long: " + txid);
        }
        try {
            return Hex.decodeHex(txid.toCharArray());
        } catch (DecoderException e) {
            throw new IllegalArgumentException("invalid txid: " + txid, e);
        }
    }

    public static SrcAddressType getBtcAddressType(Script.ScriptType scriptType) {
        switch (scriptType) {
            case P2PKH:
                return SrcAddressType.P2PKH;
            case P2SH:
                return SrcAddressType.P2SH;
            case P2WPKH:
                return SrcAddressType.P2WPKH;
            case P2WSH:
                return SrcAddressType.P2WSH;
        }
        return null;
    }

    public static void logRuntime(String name, Runnable r) {
        long s = System.currentTimeMillis();
        r.run();
        log.debug("{}. Runtime: {} msec.", name, System.currentTimeMillis() - s);
    }

    public static Properties loadProperties(String file) {
        Properties prop = new Properties();
        if (file != null) {
            try (Reader in = new FileReader(file)) {
                prop.load(in);
            } catch (Exception e) {
            }
        }
        return prop;
    }

    public static <T> T synchronize(Object syncObject, Supplier<T> supplier) {
        synchronized (syncObject) {
            return supplier.get();
        }
    }

    public static class NumberFile {

        private final File file;
        private final Long number;

        @SuppressWarnings("UseSpecificCatch")
        public NumberFile(String param) {
            if (StringUtils.isBlank(param)) {
                file = null;
                number = 0L;
            } else if (StringUtils.isNumeric(param)) {
                file = null;
                number = Long.valueOf(param);
            } else {
                file = new File(param);
                if (file.exists()) {
                    try {
                        number = Long.valueOf(FileUtils.readFileToString(file, StandardCharsets.ISO_8859_1).replaceAll("[^\\d.]", ""));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    number = 0L;
                }
            }
        }

        public void updateNumber(long n) {
            if (file != null) {
                try {
                    FileUtils.writeStringToFile(file, new DecimalFormat("#,###").format(n), StandardCharsets.ISO_8859_1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public Long getNumber() {
            return number;
        }
    }
}
