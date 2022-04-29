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
import apache.rocketmq.v2.VerifyMessageCommand;
import org.apache.rocketmq.grpcclient.route.Endpoints;

public interface Client {
    /**
     * Get the unique client identifier for each client.
     *
     * @return unique client identifier.
     */
    String getClientId();

    /**
     * Send heart beat to remote {@link Endpoints}.
     */
    void doHeartbeat();

    /**
     * Voluntary announce settings to remote.
     */
    void announceSettings() throws Exception;

    /**
     * Apply setting from remote.
     *
     * @param endpoints remote endpoints.
     * @param settings  settings received from remote.
     */
    void applySettings(Endpoints endpoints, Settings settings);

    /**
     * This method is invoked while request of printing thread stack trace is received from remote.
     *
     * @param endpoints                    remote endpoints.
     * @param printThreadStackTraceCommand request of printing thread stack trace from remote.
     */
    void onPrintThreadStackCommand(Endpoints endpoints, PrintThreadStackTraceCommand printThreadStackTraceCommand);

    /**
     * This method is invoked while request of message consume verification is received from remote.
     *
     * @param endpoints            remote endpoints.
     * @param verifyMessageCommand request of message consume verification from remote.
     */
    void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand verifyMessageCommand);

    /**
     * This method is invoked while request of orphaned transaction recovery is received from remote.
     *
     * @param endpoints                         remote endpoints.
     * @param recoverOrphanedTransactionCommand request of orphaned transaction recovery from remote.
     */
    void onRecoverOrphanedTransactionCommand(Endpoints endpoints,
        RecoverOrphanedTransactionCommand recoverOrphanedTransactionCommand);

    /**
     * Do some stats for client.
     */
    void doStats();
}
