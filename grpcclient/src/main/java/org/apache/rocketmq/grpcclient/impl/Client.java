package org.apache.rocketmq.grpcclient.impl;

public interface Client {
    /**
     * Send heart beat to remote {@link Endpoints}.
     */
    public abstract void doHeartbeat();

    /**
     * Check the status of remote {@link Endpoints}.
     */
    public abstract void doHealthCheck();

    /**
     * Do some stats for client.
     */
    public abstract void doStats();
}
