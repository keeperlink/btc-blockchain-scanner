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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Sliva Co
 */
@EqualsAndHashCode
public class Binary32 {

    @Getter
    private final byte[] data;

    public static Binary32 build(String hex) {
        checkArgument(hex != null && hex.length() == 64, "Argument 'hex' should be non-null string 64 characters long");
        return new Binary32(Utils.decodeHex(hex));
    }

    public Binary32(byte[] data) {
        checkArgument(data != null && data.length == 32, "Argument 'data' should be non-null array 32 bytes long");
        this.data = data;
    }

    @Override
    public String toString() {
        return Hex.encodeHexString(data, true);
    }

}
