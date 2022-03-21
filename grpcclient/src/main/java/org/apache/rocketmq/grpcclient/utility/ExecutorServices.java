package org.apache.rocketmq.grpcclient.utility;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServices {
    private ExecutorServices() {
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean awaitTerminated(ExecutorService executor) throws InterruptedException {
        return executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}
