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

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Sliva Co
 */
@RequiredArgsConstructor
public class ThreadFactoryWithDBConnection implements ThreadFactory, ForkJoinWorkerThreadFactory {

    private final DBConnectionSupplier dbCon;
    private final String threadName;
    private final boolean daemon;

    @Override
    public Thread newThread(Runnable runnable) {
        return new TheThread(runnable);
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return new TheForkJoinWorkerThread(pool);
    }

    private class TheThread extends Thread {

        private final Runnable runnable;

        public TheThread(Runnable runnable) {
            super();
            this.runnable = runnable;
            setName(threadName);
            setDaemon(daemon);
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } finally {
                dbCon.close();
            }
        }
    }

    private class TheForkJoinWorkerThread extends ForkJoinWorkerThread {

        public TheForkJoinWorkerThread(ForkJoinPool pool) {
            super(pool);
            setName(threadName);
            setDaemon(daemon);
        }

        @Override
        @SuppressWarnings("CallToThreadRun")
        public void run() {
            try {
                super.run();
            } finally {
                dbCon.close();
            }
        }

        @Override
        protected void onTermination(Throwable exception) {
            //dbCon.close();
            super.onTermination(exception);
        }
    }
}
