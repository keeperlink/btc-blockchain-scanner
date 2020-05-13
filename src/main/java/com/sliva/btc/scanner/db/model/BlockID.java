/*
 * Copyright 2020 Sliva Co.
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

import static com.google.common.base.Preconditions.checkArgument;
import com.sliva.btc.scanner.util.Utils;

/**
 *
 * @author Sliva Co
 */
public class BlockID extends Binary32 {

    public static BlockID build(String hex) {
        checkArgument(hex != null && hex.length() == 64, "Argument 'hex' should be non-null string 64 characters long");
        return new BlockID(Utils.decodeHex(hex));
    }

    public BlockID(byte[] data) {
        super(data);
    }

}
