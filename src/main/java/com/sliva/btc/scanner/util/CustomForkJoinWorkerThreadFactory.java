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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author whost
 */
@Slf4j
public class CustomForkJoinWorkerThreadFactory
        implements ForkJoinWorkerThreadFactory {

    private final String threadNamePrefix;
    private final AtomicInteger threadNum = new AtomicInteger();
    private final Queue<Fjwt> threadPool = new LinkedList<>();
    private boolean shutdown;

    public CustomForkJoinWorkerThreadFactory(String threadNamePrefix) {
        this.threadNamePrefix = threadNamePrefix;
    }

    @Override
    public final ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        synchronized (threadPool) {
            if (!threadPool.isEmpty()) {
                return threadPool.poll();
            }
        }
        return new Fjwt(pool);
    }

    public void shutdown() {
        shutdown = true;
        synchronized (threadPool) {
            threadPool.clear();
        }
    }

    public class Fjwt extends ForkJoinWorkerThread {

        public Fjwt(ForkJoinPool pool) {
            super(pool);
            setName(threadNamePrefix + '-' + threadNum.incrementAndGet());
        }

        @Override
        protected void onTermination(Throwable exception) {
            log.debug("CustomForkJoinWorkerThreadFactory: onTermination({}): ex: {}", getName(), exception);
            if (!shutdown) {
                synchronized (threadPool) {
                    threadPool.add(this);
                }
            }
        }

        @Override
        protected void onStart() {
            log.debug("CustomForkJoinWorkerThreadFactory: onStart({})", getName());
        }
    }
}
