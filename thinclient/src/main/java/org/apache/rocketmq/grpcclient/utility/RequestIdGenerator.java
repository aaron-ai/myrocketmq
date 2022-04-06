package org.apache.rocketmq.grpcclient.utility;

import java.util.UUID;

public class RequestIdGenerator {
    private static final RequestIdGenerator INSTANCE = new RequestIdGenerator();

    public static RequestIdGenerator getInstance() {
        return INSTANCE;
    }

    public String next() {
        return UUID.randomUUID().toString();
    }
}
