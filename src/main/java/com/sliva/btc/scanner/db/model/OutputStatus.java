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
 * @author Sliva Co
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OutputStatus {

    public static final byte UNSPENT = 0;
    public static final byte SPENT = 1;
    public static final byte UNDEFINED = 2;
    public static final byte UNSPENDABLE = 3;
    public static final byte UNSPENDABLE_OP_RETURN = 4;
    public static final byte UNSPENDABLE_BAD_SCRIPT = 5;
}
