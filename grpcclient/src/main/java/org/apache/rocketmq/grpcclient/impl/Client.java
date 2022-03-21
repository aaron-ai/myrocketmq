package org.apache.rocketmq.grpcclient.impl;

public interface Client {
    /**
     * Send heart beat to remote {@link Endpoints}.
     */
    void doHeartbeat();

    /**
     * Check the status of remote {@link Endpoints}.
     */
    void doHealthCheck();

    /**
     * Do some stats for client.
     */
    void doStats();
}
