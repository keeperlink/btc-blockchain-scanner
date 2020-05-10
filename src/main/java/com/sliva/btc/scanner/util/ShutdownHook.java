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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class ShutdownHook {

    @Getter
    private boolean interrupted;
    private boolean finished;

    public ShutdownHook() {
        this(null);
    }

    public ShutdownHook(Runnable onBreak) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutting down...");
                interrupted = true;
                if (onBreak != null) {
                    onBreak.run();
                }
                while (!finished) {
                    Utils.sleep(100);
                }
            }
        });
    }

    public void finished() {
        finished = true;
    }
}
