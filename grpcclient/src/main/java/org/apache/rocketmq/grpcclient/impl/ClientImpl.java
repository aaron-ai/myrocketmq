package org.apache.rocketmq.grpcclient.impl;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.grpcclient.utility.UtilAll;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("UnstableApiUsage")
public class ClientImpl extends AbstractIdleService implements Client {
    private static final String CLIENT_ID_SEPARATOR = "@";

    private final ClientConfiguration clientConfiguration;

    protected final String id;

    public ClientImpl(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");

        StringBuilder sb = new StringBuilder();
        final String hostName = UtilAll.hostName();
        sb.append(hostName);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(UtilAll.processId());
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(Long.toString(System.nanoTime(), 36));
        this.id = sb.toString();
    }


    @Override
    protected void startUp() {

    }

    @Override
    protected void shutDown() throws InterruptedException {

    }

    @Override
    public void doHeartbeat() {

    }

    @Override
    public void doHealthCheck() {

    }

    @Override
    public void doStats() {

    }
}
