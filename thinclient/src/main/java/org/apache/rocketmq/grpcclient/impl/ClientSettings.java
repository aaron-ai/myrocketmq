package org.apache.rocketmq.grpcclient.impl;

import apache.rocketmq.v2.Settings;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import org.apache.rocketmq.apis.retry.BackoffRetryPolicy;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public abstract class ClientSettings {
    protected final String clientId;
    protected final ClientType clientType;
    protected final Endpoints accessPoint;
    protected BackoffRetryPolicy backoffRetryPolicy;
    protected final Duration requestTimeout;
    protected final SettableFuture<Void> firstApplyCompletedFuture;

    public ClientSettings(String clientId, ClientType clientType, Endpoints accessPoint,
        BackoffRetryPolicy backoffRetryPolicy, Duration requestTimeout) {
        this.clientId = clientId;
        this.clientType = clientType;
        this.accessPoint = accessPoint;
        this.backoffRetryPolicy = backoffRetryPolicy;
        this.requestTimeout = requestTimeout;
        this.firstApplyCompletedFuture = SettableFuture.create();
    }

    public ClientSettings(String clientId, ClientType clientType, Endpoints accessPoint, Duration requestTimeout) {
        this(clientId, clientType, accessPoint, null, requestTimeout);
    }

    public abstract Settings toProtobuf();

    public abstract void applySettings(Settings settings);

    public SettableFuture<Void> getFirstApplyCompletedFuture() {
        return firstApplyCompletedFuture;
    }
}
