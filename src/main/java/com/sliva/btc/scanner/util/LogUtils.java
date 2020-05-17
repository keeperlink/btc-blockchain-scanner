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
package com.sliva.btc.scanner.util;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.cache.CacheStats;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Sliva Co
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class LogUtils {

    public static void printCacheStats(String name, CacheStats cacheStats) {
        NumberFormat nf = NumberFormat.getIntegerInstance();
        NumberFormat pf = NumberFormat.getPercentInstance();
        pf.setRoundingMode(RoundingMode.HALF_UP);
        pf.setMaximumFractionDigits(0);
        checkArgument(name != null, "Argument 'name' is null");
        checkArgument(cacheStats != null, "Argument 'cacheStats' is null");
        log.info("{} hit:{} ({}) miss:{} ({}) evict:{} load:{} ({})",
                StringUtils.leftPad(name + ".cacheStats:", 28),
                StringUtils.leftPad(nf.format(cacheStats.hitCount()), 13),
                StringUtils.leftPad(pf.format(cacheStats.hitRate()), 4),
                StringUtils.leftPad(nf.format(cacheStats.missCount()), 13),
                StringUtils.leftPad(pf.format(cacheStats.missRate()), 4),
                StringUtils.leftPad(nf.format(cacheStats.evictionCount()), 13),
                StringUtils.leftPad(nf.format(cacheStats.loadCount()), 13),
                Duration.ofSeconds(TimeUnit.NANOSECONDS.toSeconds(cacheStats.totalLoadTime())));
    }
}
