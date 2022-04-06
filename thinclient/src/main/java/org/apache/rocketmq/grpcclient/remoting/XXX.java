package org.apache.rocketmq.grpcclient.remoting;

import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Observer;
import org.apache.rocketmq.grpcclient.impl.ClientManager;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class XXX {
    private final Endpoints endpoints;
    private final ClientManager clientManager;
    private volatile StreamObserver<TelemetryCommand> requestObserver;
    private final StreamObserver<TelemetryCommand> responseObserver;
    private Object observer;

    public XXX(Endpoints endpoints, ClientManager clientManager, StreamObserver<TelemetryCommand> requestObserver,
        Object observer) {
        this.endpoints = endpoints;
        this.clientManager = clientManager;
        this.requestObserver = requestObserver;
        this.observer = observer;
        this.responseObserver = new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand telemetryCommand) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }

    public void onNext() {

    }

}
