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
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Sliva Co
 */
@EqualsAndHashCode
public class BinaryAddress {

    @Getter
    private final byte[] data;

    public static BinaryAddress build(String hex) {
        checkArgument(StringUtils.isNotEmpty(hex), "Argument 'hex' should be non-empty string");
        return new BinaryAddress(Utils.decodeHex(hex));
    }

    public BinaryAddress(byte[] data) {
        checkArgument(data != null && data.length > 0, "Argument 'data' should be non-null non-empty array");
        this.data = data;
    }

    @Override
    public String toString() {
        return Hex.encodeHexString(data, true);
    }

}
