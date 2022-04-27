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

import apache.rocketmq.v2.TelemetryCommand;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import org.apache.rocketmq.apis.exception.ClientException;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class TelemetryRequestObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryRequestObserver.class);
    private final TelemetryResponseObserver responseObserver;

    private final ClientImpl impl;
    private final Endpoints endpoints;

    public TelemetryRequestObserver(ClientImpl impl, Endpoints endpoints) {
        this.impl = impl;
        this.endpoints = endpoints;
        this.responseObserver = new TelemetryResponseObserver(impl, endpoints);
    }

    public void telemetryCommand(TelemetryCommand command) {
        Metadata metadata;
        try {
            metadata = impl.sign();
        } catch (UnsupportedEncodingException | NoSuchAlgorithmException | InvalidKeyException e) {
            // TODO: throw exception here.
            LOGGER.error("Failed to generate signature for telemetry command.", e);
            return;
        }
        StreamObserver<TelemetryCommand> requestObserver;
        try {
            requestObserver = impl.clientManager.telemetry(endpoints, metadata, Duration.ofNanos(Long.MAX_VALUE), responseObserver);
        } catch (ClientException e) {
            // TODO: throw exception here.
            LOGGER.error("Failed to generate request observer.", e);
            return;
        }
        try {
            requestObserver.onNext(command);
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
    }
}
