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
package com.sliva.btc.scanner.src;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Sliva Co
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum SrcAddressType {
    UNKNOWN(false),
    P2PKH(true),
    P2SH(true),
    P2WPKH(true),
    P2WSH(true),
    OTHER(false);

    @Getter
    private final boolean real;
}
