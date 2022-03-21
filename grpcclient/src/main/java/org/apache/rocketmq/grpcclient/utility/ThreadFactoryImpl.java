package org.apache.rocketmq.grpcclient.utility;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadFactoryImpl implements ThreadFactory {
    public static final String THREAD_PREFIX = "Rocketmq";

    private static final AtomicLong THREAD_INDEX = new AtomicLong(0);
    private final String customName;
    private final boolean daemon;

    public ThreadFactoryImpl(final String customName) {
        this(customName, false);
    }

    public ThreadFactoryImpl(final String customName, boolean daemon) {
        this.customName = customName;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, THREAD_PREFIX + customName + "-" + THREAD_INDEX.incrementAndGet());
        thread.setDaemon(daemon);
        return thread;
    }
}