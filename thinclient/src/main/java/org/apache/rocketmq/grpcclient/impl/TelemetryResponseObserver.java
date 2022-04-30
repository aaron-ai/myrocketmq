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

import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public class TelemetryResponseObserver implements StreamObserver<TelemetryCommand> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetryResponseObserver.class);

    private final Client client;
    private final Endpoints endpoints;

    public TelemetryResponseObserver(Client client, Endpoints endpoints) {
        this.client = client;
        this.endpoints = endpoints;
    }

    @Override
    public void onNext(TelemetryCommand command) {
        switch (command.getCommandCase()) {
            case SETTINGS: {
                final Settings settings = command.getSettings();
                LOGGER.info("Receive settings from remote, endpoints={}, settings={}, clientId={}", endpoints,
                            settings, client.getClientId());
                client.applySettings(endpoints, settings);
                break;
            }
            case RECOVER_ORPHANED_TRANSACTION_COMMAND: {
                final RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand = command.getRecoverOrphanedTransactionCommand();
                client.onRecoverOrphanedTransactionCommand(endpoints, recoverOrphanedTransactionCommand);
                break;
            }
            case VERIFY_MESSAGE_COMMAND: {
                final VerifyMessageCommand verifyMessageCommand = command.getVerifyMessageCommand();
                client.onVerifyMessageCommand(endpoints, verifyMessageCommand);
                break;
            }
            case PRINT_THREAD_STACK_TRACE_COMMAND: {
                final PrintThreadStackTraceCommand printThreadStackTraceCommand = command.getPrintThreadStackTraceCommand();
                client.onPrintThreadStackCommand(endpoints, printThreadStackTraceCommand);
                break;
            }
            default:
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Exception raised from stream response observer, clientId={}, endpoints={}", client.getClientId(), endpoints, throwable);

    }

    @Override
    public void onCompleted() {
    }
}
