package org.apache.rocketmq.grpcclient.impl;

import apache.rocketmq.v2.TelemetryCommand;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.stub.StreamObserver;

public class TelemetryResponseObserver implements StreamObserver<TelemetryCommand> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryResponseObserver.class);

    private final ClientImpl impl;

    public TelemetryResponseObserver(ClientImpl impl) {
        this.impl = impl;
    }

    @Override
    public void onNext(TelemetryCommand telemetryCommand) {
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Exception raised from stream response observer, clientId={}", impl.getClientId(), throwable);
    }

    @Override
    public void onCompleted() {
    }
}
