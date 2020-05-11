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
package com.sliva.btc.scanner.db.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 *
 * @author whost
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SighashType {

    public static final byte UNDEFINED = 0;
    public static final byte SIGHASH_ALL = 1;
    public static final byte SIGHASH_NONE = 2;
    public static final byte SIGHASH_SINGLE = 3;
    public static final byte SIGHASH_ANYONECANPAY = (byte) 0x80;
    public static final byte SIGHASH_ALL_ANYONECANPAY = (byte) (SIGHASH_ALL | SIGHASH_ANYONECANPAY);
    public static final byte SIGHASH_NONE_ANYONECANPAY = (byte) (SIGHASH_NONE | SIGHASH_ANYONECANPAY);
    public static final byte SIGHASH_SINGLE_ANYONECANPAY = (byte) (SIGHASH_SINGLE | SIGHASH_ANYONECANPAY);

    public static String toHexString(byte b) {
        return new String(new char[]{HEX[(0xF0 & b) >>> 4], HEX[0x0F & b]});
    }
    private static final char[] HEX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

}
