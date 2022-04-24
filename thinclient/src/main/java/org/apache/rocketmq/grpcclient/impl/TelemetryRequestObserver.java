/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.grpcclient.impl;

import apache.rocketmq.v2.ReportActiveSettingsCommand;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.SettableFuture;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import javafx.util.Pair;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class TelemetryRequestObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryRequestObserver.class);
    private volatile Pair<StreamObserver<TelemetryCommand>, TelemetryResponseObserver> pair;

    private final ClientImpl impl;
    private final Endpoints endpoints;

    public TelemetryRequestObserver(ClientImpl impl, Endpoints endpoints) {
        this.impl = impl;
        this.endpoints = endpoints;
        this.generateObserver();
    }

    private void generateObserver() {
        try {
            TelemetryResponseObserver responseObserver = new TelemetryResponseObserver(impl, endpoints);
            StreamObserver<TelemetryCommand> requestObserver = impl.clientManager.telemetry(endpoints, impl.sign(), Duration.ofNanos(Long.MAX_VALUE), responseObserver);
            this.pair = new Pair<>(requestObserver, responseObserver);
        } catch (Throwable t) {
            generateObserver();
        }
    }

    SettableFuture<Void> reportActiveSettings() {
        final ReportActiveSettingsCommand command = impl.wrapReportActiveSettingsCommand();
        final String nonce = command.getNonce();
        TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder().setReportActiveSettingsCommand(command).build();
        try {
            final StreamObserver<TelemetryCommand> requestObserver = pair.getKey();
            final TelemetryResponseObserver responseObserver = pair.getValue();
            final SettableFuture<Void> future = responseObserver.registerActiveSettingsResultFuture(nonce);
            requestObserver.onNext(telemetryCommand);
            return future;
        } catch (Throwable t) {
            return reportActiveSettings0(1);
        }
    }

    SettableFuture<Void> reportActiveSettings0(int attempt) {
        final ReportActiveSettingsCommand command = impl.wrapReportActiveSettingsCommand();
        try {
            if (!impl.endpointsIsUsed(endpoints)) {
                LOGGER.info("Current endpoints is not used, forgive retries for reporting active settings, endpoints={}, clientId={}", endpoints, impl.clientId);
            }
            TelemetryResponseObserver responseObserver = new TelemetryResponseObserver(impl, endpoints);
            final StreamObserver<TelemetryCommand> requestObserver = impl.clientManager.telemetry(endpoints, impl.sign(), Duration.ofNanos(Long.MAX_VALUE), responseObserver);
            this.pair = new Pair<>(requestObserver, responseObserver);
            final String nonce = command.getNonce();
            final SettableFuture<Void> future = responseObserver.registerActiveSettingsResultFuture(nonce);
            TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder().setReportActiveSettingsCommand(command).build();
            requestObserver.onNext(telemetryCommand);
            return future;
        } catch (Throwable t) {
            LOGGER.error("Exception raised during the retries for active settings, endpoints={}, command={}, clientId={}", endpoints, command, impl.clientId, t);
            return reportActiveSettings0(++attempt);
        }
    }

    void next(TelemetryCommand telemetryCommand) {
        try {
            final StreamObserver<TelemetryCommand> requestObserver = pair.getKey();
            requestObserver.onNext(telemetryCommand);
        } catch (Throwable t) {
            LOGGER.error("Exception raised during the invocation of client-side stream, endpoints={}, telemetryCommand={}, clientId={}", endpoints, telemetryCommand, impl.clientId, t);
            next0(telemetryCommand, 1);
        }
    }

    void next0(TelemetryCommand telemetryCommand, int attempt) {
        try {
            if (!impl.endpointsIsUsed(endpoints)) {
                LOGGER.info("Current endpoints is not used, forgive retries for telemetry command, endpoints={}, telemetryCommand={}, clientId={}", endpoints, telemetryCommand, impl.clientId);
            }
            TelemetryResponseObserver responseObserver = new TelemetryResponseObserver(impl, endpoints);
            final StreamObserver<TelemetryCommand> requestObserver = impl.clientManager.telemetry(endpoints, impl.sign(), Duration.ofNanos(Long.MAX_VALUE), responseObserver);
            this.pair = new Pair<>(requestObserver, responseObserver);
            requestObserver.onNext(telemetryCommand);
        } catch (Throwable t) {
            LOGGER.error("Exception raised during the retries for telemetry command, endpoints={}, telemetryCommand={}, clientId={}", endpoints, telemetryCommand, impl.clientId, t);
            next0(telemetryCommand, ++attempt);
        }
    }
}
